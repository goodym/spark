package chap6

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

import java.sql.{Connection, DriverManager}

object WriteDataToMysql_v3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("WriteDataToMySQL")
    val ssc = new StreamingContext(conf, Seconds(5))
    val ItemsStream = ssc.socketTextStream("c22", 8888)
    val ItemPairs = ItemsStream.map(line => (line.split(",")(0), 1))
    val ItemCount = ItemPairs.reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Seconds(60), Seconds(10))
    val hottestWord = ItemCount.transform(itemRDD => {
      val top3 = itemRDD.map(pair => (pair._2, pair._1)).sortByKey(false).map(pair => (pair._2, pair._1)).take(3)
      ssc.sparkContext.makeRDD(top3)
    })
    //优化，foreachPartition，连接MySQL是一个高消耗的事情，一个分区连接一次
    hottestWord.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val conn = getConnection()
        val stmt = conn.createStatement()
        partitionOfRecords.foreach(record => {
          val sqlins = "insert into searchKeyWord (insert_time,keyword,search_count) values (now(),'" + record._1 + "','" + record._2 + "')"
          println("sqlins:" + sqlins)
          stmt.addBatch(sqlins)
        })
        stmt.executeBatch()
        conn.commit()
      })
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  def getConnection(): Connection = {
    val url = "jdbc:mysql://home.hddly.cn:53306/test"
    val user = "test"
    val password = "test"
    Class.forName("com.mysql.jdbc.Driver")
    val conn=DriverManager.getConnection(url, user, password)
    conn.setAutoCommit(false)
    conn
  }
}
