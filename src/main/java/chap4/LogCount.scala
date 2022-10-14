package chap4

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//spark-submit :
//spark-submit --master yarn --deploy-mode client   --class   chap4.LogCount /root/word.jar /user/myname/jc_content_viewlog.txt /user/myname/out_wy_count /user/myname/out_user_count /user/myname/out_ny_count
object LogCount {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("请指定input和output")
      System.exit(1) //非0表示非正常退出程序
    }

    //TODO 1.env/准备sc/SparkContext/Spark上下文执行环境
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //TODO 2.source/读取数据
    //RDD:A Resilient Distributed Dataset (RDD):弹性分布式数据集,简单理解为分布式集合!使用起来和普通集合一样简单!
    //RDD[就是一行行的数据]
    val logs_all: RDD[Array[String]] = sc.textFile(args(0)).map {
      _.split(",")
    }
    //TODO 3.transformation/数据操作/转换
    //对访问记录中的网页去重，统计本周期内被访问网页的个数
//    478896,1043,/jszx/1043.jhtml,14884,F6D362B9AFAC436D153B7084EF3BA332,2017-03-01 00:23:07
    val wy_log: RDD[String] = logs_all.map(x => (x(1).toString)).distinct()
    val wy_count: RDD[(String, Int)] = wy_log.map(("wy_zs", _)).groupByKey().map(x => (x._1, x._2.size))
    //userid为用户注册登录的标识，对userid去重，统计登录用户的数量
    val user_log: RDD[String] = logs_all.map(x => (x(3).toString)).distinct()
    val user_count: RDD[(String, Int)] = user_log.map(("user_zs", _)).groupByKey().map(x => (x._1, x._2.size))
    //按月统计访问记录数
    val logs_all_new = logs_all.map { x => (x(0), x(1), x(2), x(3), x(4), x(5), date_time(x(5))) }
    val ny_count: RDD[(String, Int)] = logs_all_new.map(x => (x._7, 1)).reduceByKey((a, b) => a + b)

    //TODO 4.sink/输出
    //输出到指定path(可以是文件/夹)
    println("be writing to :" + args(1))
    wy_count.repartition(1).saveAsTextFile(args(1))
    println("be writing to :" + args(2))
    user_count.repartition(1).saveAsTextFile(args(2))
    println("be writing to :" + args(3))
    ny_count.repartition(1).saveAsTextFile(args(3))
    //为了便于查看Web-UI可以让程序睡一会
    Thread.sleep(1000 * 60)

    //TODO 5.关闭资源
    sc.stop()
  }

  //获取年月，时间段作为输入参数
  def date_time(date: String): String = {
    if (date.trim.length == "2017-03-01 00:23:07".length) {
      val nianye = date.trim.substring(0, 7)
      nianye
    }
    else
      "1900-01"
  }


}
