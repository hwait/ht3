package ru.otus.bigdataml.ht3

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.log4j.{Logger => SparkLogger, Level}
import java.time._
import scala.math.Ordering.Implicits._

object DataStreamer {
    SparkLogger.getRootLogger.setLevel(Level.ERROR)
    SparkLogger.getRootLogger.setLevel(Level.FATAL)
    SparkLogger.getLogger("org").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {
        implicit val localDateTimeOrdering: Ordering[LocalDateTime] = Ordering.by(x => x.atZone(ZoneId.of("UTC")).toEpochSecond)

        val conf=ProjectConfiguration()

        val props = new Properties()
        props.put("bootstrap.servers", conf.KAFKA_BROKERS)
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val kafkaProducer = new KafkaProducer[String, String](props)

        val dp=DataProvider(conf)

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
            
            val cols=DataColumns(dfDay)

            val df=dfDay
                .select("target", cols.forProducer:_*)
                .orderBy("auditedTime")
                .where(s"auditedTime>='${timeString}' and auditedTime<'${timeStringNext}'")
            if (!df.isEmpty) kafkaPush(formatContent(df))

            Thread.sleep(conf.PUSH_INTERVAL)

            if (dtNext<=conf.PUSH_END_TIME)
                if (dateString==dateStringNext)
                    iterate(dfDay, dtNext)
                else
                    iterate(dp.loadDay(dateStringNext), dtNext)
        }

        iterate(dp.loadDay(conf.date2str(conf.PUSH_START_TIME)), conf.PUSH_START_TIME)

        kafkaProducer.close()
    }

}
