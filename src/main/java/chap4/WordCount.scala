package chap4

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //以本地方式执行，可以指定线程数
    val conf = new SparkConf().setAppName("WordCount").setMaster("spark://master:7077")
//   local 、spark://master:7077
    val sc = new SparkContext(conf)
    System.setProperty("hadoop.home.dir","E:\\soft\\hadoop\\hadoop-3.3.1")
    //输入文件可以是本地 Windows 7 文件，也可以是其他来源文件，例如 HDFS
    val input = "e:\\tmp\\words.txt"
    //计算各个单词出现次数
    val count = sc.textFile(input).flatMap(x => x.split(" ")).map( x =>
      ( x,1)).reduceByKey((x,y) => x+y)
    count.foreach(x =>println(x._1+","+x._2))
  }
}
