package chap4

object HelloWorld {
  def main(args:Array[String]):Unit={
    println("Hello World")
    var str:String="478896,1043,/jszx/1043.jhtml,14884,F6D362B9AFAC436D153B7084EF3BA332,2017-03-01 00:23:07"
    var x = str.split(",")
    var str2= date_time(x(5))
    println(str2)
  }

  //获取年月，时间段作为输入参数
  def date_time(date: String): String = {
    if (date.trim.length=="2017-03-01 00:23:07".length) {
      val nianye = date.trim.substring(0, 7)
      nianye
    }
    else
      "1900-01"
  }

}
