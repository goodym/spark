import org.apache.spark.{SparkConf,SparkContext}
/**
 * Created by Administrator on 2017/9/18 0018.
 */
object ToDistribute {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test partition")
    val sc = new SparkContext(conf)
    val input = args(0)
    val output = args(1)
    val data = sc.textFile(input).map{x=>val y = x.split(",");(y(0),y(1))}
    val data2 = data.partitionBy(new MyPartition(2))
    data2.saveAsTextFile(output)
  }
}
