package ru.otus.bigdataml.ht3

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler, Normalizer}

import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, sql}
import org.apache.log4j.{Logger, Level}

object SparkMLRegressionFromKafka {
    Logger.getRootLogger.setLevel(Level.ERROR)
    Logger.getRootLogger.setLevel(Level.FATAL)
    Logger.getLogger("org").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {
        val conf=ProjectConfiguration("consumer")

        val sparkConf = new SparkConf().setAppName(conf.SPARK_APPNAME).setMaster(conf.SPARK_MASTER)
        val sparkStreamingContext = new StreamingContext(sparkConf, Seconds(conf.SPARK_BATCH_DURATION))

        val spark = SparkSession.builder.config(sparkConf).getOrCreate()
        import spark.implicits._

        val kafkaParams = Map[String, Object]("bootstrap.servers" -> conf.KAFKA_BROKERS,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> conf.KAFKA_GROUP_ID,
            "auto.offset.reset" -> conf.KAFKA_OFFSET_RESET,
            "enable.auto.commit" -> (false: java.lang.Boolean))

        val topicsSet = Set(conf.KAFKA_TOPIC)

        val kafkaStream = KafkaUtils.createDirectStream[String, String](sparkStreamingContext, PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

        kafkaStream.map(message => {
            message.value().toString
        }).foreachRDD(rdd => {
            if (!rdd.isEmpty()) {

                val data = spark.read.json(rdd.toDS())
                println(s"Data: loaded [${data.count.toDouble}:${data.columns.length}]")

                val cols=DataColumns(data)

                val encoders = (cols.categorical zip cols.categoricalClassesNum).map (
                    c => new OneHotEncoderToKnownClassesNumber(c._2).setInputCol(c._1).setOutputCol(s"${c._1}_enc")
                )

                val assemblerNumerical = (new VectorAssembler()
                            .setInputCols(cols.numerical)
                            .setOutputCol("features_numerical") )
            
                val normalizer = new Normalizer()
                    .setInputCol("features_numerical")
                    .setOutputCol("features_norm")
                    .setP(2.0)

                val scaler = new StandardScaler().setInputCol("features_norm").setOutputCol("features_scaled")

                val assemblerFinal = (new VectorAssembler()
                    .setInputCols(cols.categoricalEncoded:+"features_scaled")
                    .setOutputCol("features") )

                val pipeline = new Pipeline().setStages(encoders++Array(assemblerNumerical, normalizer, scaler, assemblerFinal))
                
                val transformed = pipeline.fit(data).transform(data)
                //transformed.select("features",cols.categoricalEncoded:_*).show(10)

                val lr = new LinearRegression()
                    .setLabelCol("target")
                    .setFeaturesCol("features_scaled")
                    .setMaxIter(100)
                    .setRegParam(1.0)

                val lrModel = lr.fit(transformed)

                println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
                val trainingSummary = lrModel.summary
                println(s"RMSE: ${trainingSummary.rootMeanSquaredError}, r2: ${trainingSummary.r2}")
            }
        })

        sparkStreamingContext.start()
        sparkStreamingContext.awaitTermination()

    }

}
