package ru.otus.bigdataml.ht3

import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler,MinMaxScaler, Normalizer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.{Pipeline, PipelineStage}

/*
 * Реализация Пайплайна с возможностью вариаций. 
 */

// Интерфейс изменяемых и реализация константных стадий
trait DataTransformer extends DataColumns {
  def categoricalEncoders(): Array[OneHotEncoderToKnownClassesNumber] = categoricalColumnsWithClassesNum.map (
        c => new OneHotEncoderToKnownClassesNumber(c._2).setInputCol(c._1).setOutputCol(s"${c._1}_enc")
    )
  
  def numericalAssembler(): VectorAssembler = new VectorAssembler()
    .setInputCols(numericalColumns)
    .setOutputCol(numericalAssemblerOutputColumn) 

  def normalizer(p: Double): Normalizer = new Normalizer()
    .setInputCol(numericalAssemblerOutputColumn)
    .setOutputCol(normalizerOutputColumn)
    .setP(p)

  def scaler(): PipelineStage

  def finalAssembler(): VectorAssembler = new VectorAssembler()
    .setInputCols(categoricalEncodedColumns:+scalerOutputColumn)
    .setOutputCol(featuresColumn) 
}

// Переопределение вариаций
class DataTransformerStandard extends DataTransformer {
  override def scaler() = new StandardScaler()
  .setInputCol(normalizerOutputColumn)
  .setOutputCol(scalerOutputColumn)
}

class DataTransformerMinMax extends DataTransformer {
  override def scaler() = new MinMaxScaler()
  .setInputCol(normalizerOutputColumn)
  .setOutputCol(scalerOutputColumn)
}

// Увязывание стадий в пайплайн
case class DataTransformerHelper(transformer: DataTransformer)  {
  def transform(df: DataFrame): DataFrame = {
    val pipeline = new Pipeline().setStages(
      transformer.categoricalEncoders++Array(
        transformer.numericalAssembler, 
        transformer.normalizer(2.0), 
        transformer.scaler, 
        transformer.finalAssembler
    ))
        
    pipeline.fit(df).transform(df)
  }
}

// Выбор вариации по ключу
object DataTransformerService {
  def apply(transformerType: String): DataTransformer = {
    transformerType match {
      case "minmax" => new DataTransformerMinMax()
      case _ => new DataTransformerStandard()
    }
  }
}