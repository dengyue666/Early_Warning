import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object CreateKafka {

  //调用 Cold_Test_SreamContext 获取 streamingContext
  private val streamingContext: StreamingContext = KafkaC.streamingContext

  // 从kafka里面吧数据获取下来
  // 指定要从哪个主题下拉去数据
  private val servers = KafkaC.config.getString("cold_test.kafka.brokers")
  private val topics = Array(KafkaC.config.getString("cold_test.kafka.topics3"))
  private val groupId = KafkaC.config.getString("cold_test.kafka.group3")
  private val protocol = "SASL_PLAINTEXT"
  private val kerberos_service_name = "kafka"
  private val mechanism = "GSSAPI"
  // 指定kafka相关参数
  private val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> servers,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupId,
            "auto.offset.reset" -> "earliest",
            "enable.auto.commit" -> (false: java.lang.Boolean),
            "security.protocol" -> protocol,
            "sasl.kerberos.service.name" -> kerberos_service_name,
            "sasl.mechanism" -> mechanism)

  val HiveKafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
  )

}
