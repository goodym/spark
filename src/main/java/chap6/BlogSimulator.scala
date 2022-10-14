package chap6
import java.io._
import java.text.SimpleDateFormat
import java.util.Date
import java.io.PrintWriter
import scala.io.Source
object BlogSimulator {
  def main(args: Array[String]) {
    var i=0
    while (true)
    {
      val filename="D:\\tmp\\spark\\test.log"
      val outputpath="D:\\tmp\\spark\\streaming\\"
      val lines = Source.fromFile(filename).getLines.toList
      val filerow = lines.length
      val writer = new PrintWriter(new File(outputpath+"streamingdata "+i+".txt" ))
      i=i+1
      var j=0
      while(j<100)
      {
        writer.write(lines(index(filerow))+"\n")
        println(lines(index(filerow)))
        j=j+1
      }
      writer.close()
      Thread sleep 5000
      log(getNowTime(),outputpath+"streamingdata"+i+".txt generated")
    }
  }
  def log(date: String, message: String)  = {
    println(date + "----" + message)
  }
  def index(length: Int) = {
    import java.util.Random
    val rdm = new Random
    rdm.nextInt(length)
  }
  def getNowTime():String={
    val now:Date = new Date()
    val datetimeFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val ntime = datetimeFormat.format( now )
    ntime
  }
}
