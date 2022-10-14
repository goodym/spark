import org.apache.spark.{SparkConf, SparkContext}

object WordCount1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    val input = args(0)
    val output = args(1)
    //计算各个单词出现次数
    val count = sc.textFile(input).flatMap(x => x.split(" ")).map(x =>
      (x, 1)).reduceByKey((x, y) => x + y)
    count.foreach(x => println(x._1 + "," + x._2))
    count.repartition(1).saveAsTextFile(output)
  }
}
