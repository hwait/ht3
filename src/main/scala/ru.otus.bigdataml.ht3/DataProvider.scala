package ru.otus.bigdataml.ht3

import org.apache.spark.sql.{SparkSession, DataFrame}
import scala.reflect.runtime.universe._
import org.apache.spark.sql.functions._

/*
* Предоставляет данные для Продьюсера и тестового тренинга. Использует конфигурацию Продьюсера.
*/

class DataProvider() extends ProducerConfiguration {

  val spark=SparkSession.builder()
    .appName(SPARK_APPNAME)
    .master(SPARK_MASTER)
    .getOrCreate()

  import spark.implicits._

    /**
    * Для уменьшения трафика сделаем индексацию категориальных значений в заранее известное количество классов.
    * Генератор UDF для пользовательской индексации категорий: 
    * Получает список категорий как шаблон и заменяет значение на индекс соответствующей категории.
    * Шаблон должен содержать все возможные значения. Все категории, не входящие в шаблон, помещаются 
    * в последний класс (его номер равен длине массива шаблона)
    */

  def indexer[T](xs:List[T])(implicit typeTag: TypeTag[T]) = {
      val mapInstanceId:Map[T,Int] = (xs zip xs.indices).toMap
      udf( (x: T) => mapInstanceId.getOrElse(x,xs.length) )  
  }

  // Для стриминга необязательно загружать все данные, будем подгружать их по дням.
  def loadDay(day:String):DataFrame = {
      val dfLoaded=spark.read.parquet(DATA_PATH).filter(s"date='${day}'")
      val DF_COUNT=dfLoaded.count.toDouble
      println(s"Day ${day} loaded ${DF_COUNT} rows.")
      
      // Получаем UDF для каждого категориального класса
      val translateInstanceId = indexer[String](List("Post", "Photo", "Video"))
      val translateOwnerType = indexer[String](List("GROUP_OPEN_OFFICIAL", "GROUP_OPEN"))
      val translateMembershipStatus = indexer[String](List("!","A","B","I","M","P","R","Y"))
      val translateResourceType = indexer[Int](List(3,6,7,8,14))

      dfLoaded
          .withColumn("target", when(array_contains($"feedback", "Liked"), lit(1)).otherwise(lit(0)))
          .withColumn("createdTime", from_unixtime($"metadata_createdAt" / 1000))
          .withColumn("auditedTime", from_unixtime($"audit_timestamp" / 1000))
          .withColumn("timeDelta", (($"audit_timestamp" - $"metadata_createdAt") / 3600000 /24).cast("integer"))
          .withColumn("createdHour", hour($"createdTime"))
          .withColumn("auditedHour", hour($"auditedTime"))
          .withColumn("log_ownerId", round(log10($"metadata_ownerId")))
          .withColumn("log_authorId", round(log10($"metadata_authorId")))
          .withColumn("instanceId_objectType_num", translateInstanceId($"instanceId_objectType"))
          .withColumn("audit_resourceType_num", translateResourceType($"audit_resourceType"))
          .withColumn("metadata_ownerType_num", translateOwnerType($"metadata_ownerType"))
          .withColumn("membership_status_num", translateMembershipStatus($"membership_status"))
          .na.fill(0) // Заполняем пустые значения после индексации категорий, чтобы они не попали в нулевой класс
  }
}
