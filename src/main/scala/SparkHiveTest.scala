import org.apache.spark.sql.{DataFrame, SparkSession}


object SparkHiveTest {

  def main(args: Array[String]) {
    val sparkHiveSession: SparkSession = SparkSession.builder()
      .config("spark.defalut.parallelism", 200)
      .config("spark.sql.shuffle.partitions", 200)
      .config("spark.speculation", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.rdd.compress", "true")
      .config("hive.metastore.sasl.enabled", "true")
      .config("spark.executorEnv.PYTHONHASHSEED", 0)
      .enableHiveSupport()
      .getOrCreate()

    val frame: DataFrame = sparkHiveSession.sql("select * from datalake_dms.dw_dealer_dms_srvc_so_logla limit 10")
    frame.rdd.foreach(data =>{
      data
    })
    sparkHiveSession.stop()


  }
}
