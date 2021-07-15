package ru.otus.bigdataml.ht3

import org.apache.spark.ml.regression.LinearRegression
//import org.slf4j.{Logger, LoggerFactory}
import org.apache.log4j.{Logger => SparkLogger, Level}

/*
*     Для тестирования на локальных данных (без стриминга)
*/
object SparkMLModelTraining extends DataColumns {
    SparkLogger.getRootLogger.setLevel(Level.ERROR)
    SparkLogger.getRootLogger.setLevel(Level.FATAL)
    SparkLogger.getLogger("org").setLevel(Level.WARN)
    
    //val LOGGER: Logger = LoggerFactory.getLogger(SparkMLModelTraining.getClass.getName)
    
    def main(args: Array[String]): Unit = {

        val conf=new ProducerConfiguration()
        
        val dp=new DataProvider()

        val df = dp.loadDay(conf.date2str(conf.PUSH_START_TIME))
       
        println(s"Data: loaded [${df.count.toDouble}:${df.columns.length}]")

        val transformer = new DataTransformerHelper(DataTransformerService("standard"))
        val dfTransformed = transformer.transform(df)

        dfTransformed.select(featuresColumn,categoricalEncodedColumns:_*).show(10)

        val lr = new LinearRegression()
            .setLabelCol(targetColumn)
            .setFeaturesCol(featuresColumn)
            .setMaxIter(100)
            .setRegParam(1.0)

        val lrModel = lr.fit(dfTransformed)

        println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
        val trainingSummary = lrModel.summary
        println(s"numIterations: ${trainingSummary.totalIterations}")
        println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
        println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
        println(s"r2: ${trainingSummary.r2}")
 
    }

}
