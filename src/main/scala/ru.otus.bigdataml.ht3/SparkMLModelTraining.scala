package ru.otus.bigdataml.ht3

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler, Normalizer}
import org.apache.spark.sql.functions._
//import org.slf4j.{Logger, LoggerFactory}
import org.apache.log4j.{Logger => SparkLogger, Level}

object SparkMLModelTraining {
    SparkLogger.getRootLogger.setLevel(Level.ERROR)
    SparkLogger.getRootLogger.setLevel(Level.FATAL)
    SparkLogger.getLogger("org").setLevel(Level.WARN)
    
    //val LOGGER: Logger = LoggerFactory.getLogger(SparkMLModelTraining.getClass.getName)
    
    def main(args: Array[String]): Unit = {
        val conf=ProjectConfiguration()
        val dp=DataProvider(conf)

        val df = dp.loadDay(conf.date2str(conf.PUSH_START_TIME))

        val cols=DataColumns(df)

        println(s"Data: loaded [${df.count.toDouble}:${df.columns.length}]")

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
        
        val transformed = pipeline.fit(df).transform(df)
        transformed.select("features",cols.categoricalEncoded:_*).show(10)

        val lr = new LinearRegression()
            .setLabelCol("target")
            .setFeaturesCol("features_scaled")
            .setMaxIter(100)
            .setRegParam(1.0)

        val lrModel = lr.fit(transformed)

        println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
        val trainingSummary = lrModel.summary
        println(s"numIterations: ${trainingSummary.totalIterations}")
        println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
        println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
        println(s"r2: ${trainingSummary.r2}")
 
    }

}
