package ru.otus.bigdataml.ht3

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Column}

class OneHotEncoderToKnownClassesNumber(
    classesNumber: Int,
    override val uid: String
) extends Transformer {
  final val inputCol = new Param[String](this, "inputCol", "The input column")
  final val outputCol =
    new Param[String](this, "outputCol", "The output column")

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def this(n: Int) = this(n, Identifiable.randomUID("OHEncodingTopN"))

  def copy(extra: ParamMap): OneHotEncoderToKnownClassesNumber = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    schema.add(StructField($(outputCol), IntegerType, false))
  }

  def vectorize(x: Double) = {
    val idx = x.toInt
    if (idx < classesNumber) {
      Vectors.sparse(classesNumber, Array(idx), Array(1.0))
    } else {
      Vectors.sparse(classesNumber, Array.emptyIntArray, Array.emptyDoubleArray)
    }
  }

  val vectorize_udf = udf(vectorize _)

  def transform(df: Dataset[_]): DataFrame = {
    df.withColumn($(outputCol), vectorize_udf(col($(inputCol))))
  }
}
