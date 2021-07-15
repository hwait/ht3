package ru.otus.bigdataml.ht3

import com.typesafe.config.{Config, ConfigFactory}
import java.time.format.DateTimeFormatter
import java.time._

/*
 * Конфигуратор для сокращения кода, в двух версиях
 */

trait ProjectConfiguration {
    val config: Config = ConfigFactory.load("application.json")

    val timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    def date2str(x:LocalDateTime):String = dateFormatter.format(x)
    def time2str(x:LocalDateTime):String = timeFormatter.format(x)
    
    val KAFKA_TOPIC: String = config.getString("kafka.producer_topic")
    val KAFKA_BROKERS: String = config.getString("kafka.brokers")
    val KAFKA_GROUP_ID: String = config.getString("kafka.group_id")
    val KAFKA_OFFSET_RESET: String = config.getString("kafka.auto_offset_reset")
    val DATA_PATH: String = config.getString("hdfs.storage_location")
    val SPARK_MASTER: String = config.getString("spark.master")
    val SPARK_APPNAME: String
    val SPARK_BATCH_DURATION: Int = config.getInt("spark.batch_duration")
    val PUSH_START_TIME: LocalDateTime = LocalDateTime.parse(config.getString("push.start_time"), timeFormatter)    
    val PUSH_END_TIME: LocalDateTime = LocalDateTime.parse(config.getString("push.end_time"), timeFormatter)    
    val PUSH_INTERVAL: Int = config.getInt("push.interval_ms")
    val PUSH_BATCH: Int = config.getInt("push.batch_duration_s")
}

class ProducerConfiguration extends ProjectConfiguration {
  override val SPARK_APPNAME: String = config.getString("spark.app_name_producer")
}

class ConsumerConfiguration extends ProjectConfiguration {
  override val SPARK_APPNAME: String = config.getString("spark.app_name_consumer")
}

