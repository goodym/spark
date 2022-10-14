package chap6

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import utils.ConnectionPool

import java.sql.{Connection, DriverManager}

object WriteDataToMysql_v4 {
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
    //优化，foreachPartition，增加连接池，执行后不关闭连接，返回到连接池中
    hottestWord.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val connection = ConnectionPool.getConnection().get
        partitionOfRecords.foreach(record => {
          val sqlins = "insert into searchKeyWord (insert_time,keyword,search_count) values (now(),'" + record._1 + "','" + record._2 + "')"
          println("sqlins:" + sqlins)
          connection.createStatement().execute(sqlins)
        })
        ConnectionPool.returnConnection(connection)
      })
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
