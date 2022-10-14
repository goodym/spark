package stock

import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import scala.collection.mutable

object CalculateChange {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("calculate zhangfu").setMaster("local")
    val sc = new SparkContext(conf)
    val input = args(0)
    val output = args(1)
    val splitter = args(2)
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    val data = sc.textFile(input).filter(x => !x.contains("date")).map { x =>
      val split_data = x.split(splitter);
      val timestamp = fm.parse(split_data(0));
      (timestamp.getTime(), (split_data(0), split_data(3).toDouble))
    }.filter(x => x._2._2 != 0).sortByKey().map(x => (x._2._1, x._2._2))
    val queue = new mutable.Queue[Double]()
    val percentFormat = java.text.NumberFormat.getPercentInstance
    val zhangfu = data.map { x =>
      queue.enqueue(x._2);
      if (queue.size == 2) {
        val cha = (queue.last - queue.head) / queue.head;
        queue.dequeue();
        (x._1, cha)
      }
      else {
        (0, 0)
      }
    }
    val result = zhangfu.filter(x => x._1 != 0).map(x => x._1 + "," + x._2)
    result.repartition(1).saveAsTextFile(output)
  }
}
