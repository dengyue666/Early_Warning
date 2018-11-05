import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark

object KafkaC {

  val spark: SparkSession = SparkSession.builder()
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.defalut.parallelism", 1)
    .config("spark.sql.shuffle.partitions", 100)
    .config("spark.speculation", "true")
    .config("spark.rdd.compress", "true")
    .config("spark.streaming.concurrentJobs", 200)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.executorEnv.PYTHONHASHSEED", 0)
    .config("cluster.name", "")
    .config("es.index.auto.create", "true")
    .config("es.nodes", "")
    .config("es.port", "9200")
    .config("es.index.read.missing.as.empty", "true")
    .config("es.net.http.auth.user", "") //访问es的用户名
    .config("es.net.http.auth.pass", "") //访问es的密码
    .config("es.nodes.wan.only", "true")
    .getOrCreate()
  val streamingContext = new StreamingContext(spark.sparkContext, Seconds(10))
  val config: Config = ConfigFactory.load()

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)
    val hiveKafkaDStream = CreateKafka.HiveKafkaDStream
    hiveKafkaDStream.foreachRDD(rdd => {
      println("进来了")
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      rdd.foreach(line => {
//        println(line.value())
//      })

      val value: RDD[String] = rdd.map(line => {
        val str = line.value()
        str
      })
//      import spark.implicits._
//      val frame: DataFrame = value.toDF("","","")
      EsSpark.saveToEs(value,"type")




      hiveKafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def write2Es(sc: SparkContext) = {

  }
}
