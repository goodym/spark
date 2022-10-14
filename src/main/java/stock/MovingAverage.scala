package stock

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import scala.collection.mutable

object MovingAverage {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MovingAverage").setMaster("local")
    val sc = new SparkContext(conf)

    val input = args(0)
    val output = args(1)
    val splitter = args(2)
    val brodcastWindow = args(3).toInt
    val output2 = args(4)
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    val datematch = "[1-2][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]"

    val data = sc.textFile(input).map { x =>
      val split_data = x.split(splitter);
      if (!split_data(0).matches(datematch)) {
        split_data(0) = "1900-01-01";
      }
      if (!split_data(1).matches(".*\\d+.*")) {
        split_data(1) = "0.0";
      }
      val timestamp = fm.parse(split_data(0));
      (timestamp.getTime(), (split_data(0), split_data(1).toDouble))
    }.sortByKey().map(x => (x._2._1, x._2._2))
    val queue = new mutable.Queue[Double]()
    val yucezhangfu = data.map { x =>
      queue.enqueue(x._2)
      if (queue.size > brodcastWindow) {
        val ave = (queue.sum - x._2) / brodcastWindow;
        queue.dequeue();
        (x._1, ave, x._2)
      }
      else {
        (0, 0, 0)
      }
    }.filter(x => x._1 != 0).persist(StorageLevel.MEMORY_AND_DISK)
    yucezhangfu.map(x => (x._1, (x._2, x._3))).partitionBy(new DatePartition()).saveAsTextFile(output)
    yucezhangfu.map(x => (x._1, x._2)).partitionBy(new DatePartition()).saveAsTextFile(output2)
    //  val wucha = yucezhangfu.map(x=>math.abs(x._2.toString.toDouble-x._3.toString.toDouble))
    // sc.parallelize(Array(wucha.sum()/wucha.count())).saveAsTextFile(wucha_file)
    //String s="Spark";
    "abc".contains("a")
  }
}
