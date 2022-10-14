package chap6

import java.sql.{Connection, DriverManager}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BlogHotPage {
  def main(args: Array[String]) = {
    val sc = new SparkConf().setMaster("local[4]").setAppName("pagehot")
    val ssc = new StreamingContext(sc, Seconds(5))
    val lines=ssc.textFileStream("D:\\tmp\\spark\\streaming\\")
    //计算网页热度
    val html = lines.map { line => val words = line.split(","); (words(0), 0.1 * words(1).toInt + 0.9 * words(2).toInt + 0.4 * words(3).toDouble + words(4).toInt) }
    //计算每个网页的热度总和
    //html.reduceByKeyAndWindow为基于滑动窗口对(K,V)键值对类型的DStream中的值按K使用聚合函数
    val htmlCount = html.reduceByKeyAndWindow((v1: Double, v2: Double) => v1 + v2, Seconds(60), Seconds(10))
    //按照网页的热度总和降序排序
    //通过对源htmlCount的每个RDD应用RDD-to-RDD函数返回一个新的DStream
    // itemRDD.map(pair=>(pair._2,pair._1)) 是进行key,value交换，得到JavaPairRDD对象
    //JavaPairRDD的API参考:https://spark.apache.org/docs/3.1.3/api/scala/org/apache/spark/api/java/JavaPairRDD.html
    //JavaPairRDD的sortByKey(false)是对key进行排序,false表示不按升序，即按降序排
    //rdd.sortByKey(false).map (pair=>(pair._2,pair._1))又一次对key，value交换，换回原来的key
    //rdd.take(10),对排序后的结果，取前10条
    val hottestHtml = htmlCount.transform(itemRDD => {
      val top10 = itemRDD.map(pair => (pair._2, pair._1)).sortByKey(false).map(pair => (pair._2, pair._1)).take(10)
      ssc.sparkContext.makeRDD(top10).repartition(1)
    })
    hottestHtml.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val url = "jdbc:mysql://home.hddly.cn:53306/test"
        val user = "test"
        val password = "test"
        val conn = getConn(url, user, password)
        delete(conn, "top_web_page")
        conn.setAutoCommit(false)
        val stmt = conn.createStatement()
        var i = 1
        partitionOfRecords.foreach(record => {
          println("input data is " + record._1 + " " + record._2)
          stmt.addBatch("insert into top_web_page(rank,htmlID,pageheat) values ('" + i + "','" + record._1 + "','" + record._2 + "')")
          i += 1
        })
        stmt.executeBatch()
        conn.commit()
      })
    })
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  def getConn(url: String, user: String, password: String): Connection = {
    Class.forName("com.mysql.jdbc.Driver")
    val conn = DriverManager.getConnection(url, user, password)
    return conn
  }

  def delete(conn: Connection, table: String) = {
    val sql = "delete from " + table + " where 1=1"
    conn.prepareStatement(sql).executeUpdate()
  }
}
