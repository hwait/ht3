package ru.otus.bigdataml.ht3

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.log4j.{Logger => SparkLogger, Level}
import java.time._
import scala.math.Ordering.Implicits._

/*
*   Генерирует тестовые данные, последовательно подгружая однодневные блоки данных в спарк.
*   Несколько преобразованные данные стримятся в Кафку порциями заданного возраста с заданной периодичностью. 
*/

object DataProducer extends DataColumns {
    SparkLogger.getRootLogger.setLevel(Level.ERROR)
    SparkLogger.getRootLogger.setLevel(Level.FATAL)
    SparkLogger.getLogger("org").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {
        implicit val localDateTimeOrdering: Ordering[LocalDateTime] = Ordering.by(x => x.atZone(ZoneId.of("UTC")).toEpochSecond)

        // Подружаем конфигурацию Продьюсера
        val conf=new ProducerConfiguration()

        val props = new Properties()
        props.put("bootstrap.servers", conf.KAFKA_BROKERS)
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val kafkaProducer = new KafkaProducer[String, String](props)

        val dp=new DataProvider()

        def iterate(dfDay: DataFrame, dt: LocalDateTime): Unit = {
            val dtNext=dt.plusSeconds(conf.PUSH_BATCH)           // Записи собираются в батчи по auditedTime
            val timeString=conf.time2str(dt)
            val dateString=conf.date2str(dt)
            val timeStringNext=conf.time2str(dtNext)
            val dateStringNext=conf.date2str(dtNext)

            def kafkaPush(content: String): Unit = {
                val record = new ProducerRecord(conf.KAFKA_TOPIC, "key", content)
                kafkaProducer.send(record)
                println(s"Pushed Record for ${timeString}: " + content)
            }

            def formatContent(df: DataFrame): String = "["+df.toJSON.collect().mkString(",")+"]"
            
            val df=dfDay
                .select(targetColumn, exportColumns:_*)
                .orderBy(timeColumn)
                .where(s"${timeColumn}>='${timeString}' and ${timeColumn}<'${timeStringNext}'")
            if (!df.isEmpty) kafkaPush(formatContent(df))

            Thread.sleep(conf.PUSH_INTERVAL)

            if (dtNext<=conf.PUSH_END_TIME)
                if (dateString==dateStringNext)
                    iterate(dfDay, dtNext)
                else
                    iterate(dp.loadDay(dateStringNext), dtNext)
        }

        // Первый раз запускаем итерацию со стартового времени
        iterate(dp.loadDay(conf.date2str(conf.PUSH_START_TIME)), conf.PUSH_START_TIME)

        kafkaProducer.close()
    }

}
