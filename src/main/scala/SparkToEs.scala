import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql.EsSparkSQL

object SparkToEs{

  def main(args: Array[String]): Unit = {
    var sconf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[5]")
    var sc = new SparkContext(sconf)

    sconf.set("es.nodes", "127.0.0.1")
    sconf.set("es.port","9200")
    sconf.set("es.index.auto.create", "true")
    sconf.set("es.write.operation", "index")
    sconf.set("es.mapping.id", "id")
    //如果装有x-pack 可以使用下面方式添加用户名密码
   // sconf.set("es.net.http.auth.user","username")
   // sconf.set("es.net.http.auth.pass","password")
    val a:DataFrame=null
    //第一个dataframe 第二个形参格式 _index/_type
    EsSparkSQL.saveToEs(a,"testsparkes/testsparkes")
  }

}