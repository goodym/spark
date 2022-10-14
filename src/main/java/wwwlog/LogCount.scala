package wwwlog

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}

//spark-submit :
//spark-submit --master yarn --deploy-mode client   --class   chap4.LogCount /root/word.jar hdfs://master:9864/user/myname/bigdata.hddly.cn-access_log hdfs://master:9864/user/myname/output_wwwlog1 hdfs://master:9864/user/myname/output_wwwlog2 hdfs://master:9864/user/myname/output_wwwlog3 hdfs://master:9864/user/myname/output_wwwlog4
object LogCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("请指定input和output")
      System.exit(1) //非0表示非正常退出程序
//      args.[0]= hdfs://master:9864/user/myname/bigdata.hddly.cn-access_log
//      args.[1] =user/myname/output_wwwlog1
//      args.[2] =user/myname/output_wwwlog2

    }

    val conf: SparkConf = new SparkConf().setAppName("logcount").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val hdfsconf = new Configuration()
    hdfsconf.set("fs.defaultFS", "hdfs://master:9864")

    val input=args(0)
    val lines = sc.textFile(input)
    // 47.103.127.68 - - [07/Oct/2021:11:43:28 +0800] "GET / HTTP/1.1" 200 537 "-"
    // "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"

    // 1. 总PV
    val pv=lines.count()
    println("pv:"+pv)

//    // 2,词频统计
//    val word_count = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
//      .map(e => (e._2, e._1)).reduceByKey(_ + "," + _)
//      .sortByKey(true, 1)

    // 2. 各IP的PV，按PV倒序
    //   空格分隔的第一个字段就是IP
    val ips = lines.filter(_.split(" ").size > 10).map(x => (x.split(" ")(0), 1))
    val resip: RDD[(String, Int)] = ips.reduceByKey(_ + _)
    val sortip = resip.sortBy(_._2, false)
    val topip = sortip.take(10)
    topip.foreach(println)

    // 3. 取网站访问前10名
    val urls=lines.filter(_.split(" ").size>10).map(x=>(x.split(" ")(10),1))
    val resurl:RDD[(String,Int)]=urls.reduceByKey(_+_)
    val sorturl=resurl.sortBy(_._2,false)
    val topurl=sorturl.take(10)
    topurl.foreach(println)

    // 4. 取访问日期
    val dates = lines.filter(_.split(" ").size > 10).map(x => (x.split(" ")(3).substring(1,12), 1))
    val resdatestr: RDD[(String, Int)] = dates.reduceByKey(_ + _)
//    val date = LocalDateTime.parse("2017-01-01 13:14:59",DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
//    07/Oct/2021:11:43:28
    //输入日期格式
    val YYYYMMDDHHMM_TIME_FORMAT = new SimpleDateFormat("dd/MMM/yyyy", Locale.ENGLISH)
    // "dd/MMM/yyyy:HH:mm:ss Z"
    //目标日期格式
    val TARGET_TIME_FORMAT = new SimpleDateFormat("yyyyMMdd")

    val resdate:RDD[(String, Int)] = resdatestr.map(x=>(TARGET_TIME_FORMAT.format(YYYYMMDDHHMM_TIME_FORMAT.parse(x._1)),x._2))
    val sortdate = resdate.sortBy(_._1.trim,true)
    val topdate = sortdate.take(10)
    topdate.foreach(println)

    // 4. 取访问时
    val hours = lines.filter(_.split(" ").size > 10).map(x => (x.split(" ")(3).substring(13, 15), 1))
    val reshour: RDD[(String, Int)] = hours.reduceByKey(_ + _)
    val sorthour = reshour.sortBy(_._1, true)
    val tophour = sorthour.take(10)
    tophour.foreach(println)



    //输出到指定path(可以是文件/夹)
//    println("be writing to :" + args(1))
//    var output_word=args(1)
//    word_count.repartition(1).saveAsTextFile(output_word)
    println("be writing to :" + args(1))
    FileSystem.get(hdfsconf).delete(new Path(args(1)), true)
    sortip.repartition(1).saveAsTextFile(args(1))
    println("writed to :" + args(1))


    println("be writing to :" + args(2))
    FileSystem.get(hdfsconf).delete(new Path(args(2)), true)
    sorturl.repartition(1).saveAsTextFile(args(2))
    println("writed to :" + args(2))

    println("be writing to :" + args(3))
    FileSystem.get(hdfsconf).delete(new Path(args(3)), true)
    sortdate.repartition(1).saveAsTextFile(args(3))
    println("writed to :" + args(3))

    println("be writing to :" + args(4))
    FileSystem.get(hdfsconf).delete(new Path(args(4)), true)
    sorthour.repartition(1).saveAsTextFile(args(4))
    println("writed to :" + args(4))

//    println("be writing to :" + args(3))
//    ny_count.repartition(1).saveAsTextFile(args(3))
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
