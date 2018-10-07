package d_streams

import java.lang.Boolean

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object Ex_2_Kafka_to_Console {

  def main(args: Array[String]): Unit = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: Boolean)
    )

    val conf = new SparkConf().setAppName("DStream counter").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))

    val topicName = "messages"
    val topics = Array(topicName)

    val stream = KafkaUtils.createDirectStream(ssc, PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

//    val value = stream.map(record => (record.key, record.value))
//    println(value)

//    stream.foreachRDD { rdd =>
//      rdd.foreachPartition { iter =>
//        // make sure connection pool is set up on the executor before writing
//        SetupJdbc(jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword)
//
//        iter.foreach { case (key, msg) =>
//          DB.autoCommit { implicit session =>
//            // the unique key for idempotency is just the text of the message itself, for example purposes
//            sql"insert into idem_data(msg) values (${msg})".update.apply
//          }
//        }
//      }
//    }

    stream.foreachRDD { rdd =>
      println("New RDD : " + rdd)
      println("New RDD name : " + rdd.name)
      println("New RDD count : " + rdd.count())
      rdd.foreach(cr => println ("value: " + cr.value()))
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

