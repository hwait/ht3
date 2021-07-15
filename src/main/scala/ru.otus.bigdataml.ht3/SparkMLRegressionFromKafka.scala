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

/*
* Получает данные из Кафки, преобразует их с помщью пайплайна, 
* считает линейную регрессию окнами с заданной длительностью.
* Выводит для каждого батча RMSE и R2 метрики.
*/

object SparkMLRegressionFromKafka extends DataColumns {
    Logger.getRootLogger.setLevel(Level.ERROR)
    Logger.getRootLogger.setLevel(Level.FATAL)
    Logger.getLogger("org").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {
        val conf=new ConsumerConfiguration()

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

                val df = spark.read.json(rdd.toDS())
                println(s"Data: loaded [${df.count.toDouble}:${df.columns.length}]")

                val transformer = new DataTransformerHelper(DataTransformerService("standard"))
                val dfTransformed = transformer.transform(df)

                val lr = new LinearRegression()
                    .setLabelCol(targetColumn)
                    .setFeaturesCol(featuresColumn)
                    .setMaxIter(100)
                    .setRegParam(1.0)

                val lrModel = lr.fit(dfTransformed)

                println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
                val trainingSummary = lrModel.summary
                println(s"RMSE: ${trainingSummary.rootMeanSquaredError}, R2: ${trainingSummary.r2}")
            }
        })

        sparkStreamingContext.start()
        sparkStreamingContext.awaitTermination()

    }

}
