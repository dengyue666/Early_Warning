import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object KafkaTest {
  var TOPIC_NAME = "csvtest3_1"

  def main(args: Array[String]): Unit = {
    //    System.setProperty("java.security.krb5.conf", "/home/a133")
    //    System.setProperty("java.security.auth.login.config", "")
    //    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
    //       System.setProperty("sun.security.krb5.debug","true");
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "scdca0000331.cn.isn.corpintra.net:6667,scdca0000332.cn.isn.corpintra.net:6667,scdca0000333.cn.isn.corpintra.net:6667,scdca0000334.cn.isn.corpintra.net:6667")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put("security.protocol", "SASL_PLAINTEXT")
    props.put("sasl.mechanism", "GSSAPI")
    props.put("sasl.kerberos.service.name", "kafka")
    val producer = new KafkaProducer[String, String](props)
    for (i <- 1 to 10) {
      val key = "key-" + i
      val message = "Message-" + i
      val record = new ProducerRecord[String, String](TOPIC_NAME, key, message)
      producer.send(record)
      System.out.println(key + "----" + message)
    }
    producer.close()
  }
}
