package stock

import org.apache.spark.Partitioner
class DatePartition extends Partitioner{
  override def numPartitions: Int = 4
  override def getPartition(key: Any): Int = {
    val date = key.toString().substring(0,4).trim()
    if (date=="2013") {
      0
    }
    else if(date=="2014"){
      1
    }
    else if(date=="2015"){
      2
    }else{
      3
    }
  }
  override def equals(other: Any): Boolean = other match {
    case datepartition: DatePartition =>
      datepartition.numPartitions == numPartitions
    case _ =>
      false
  }
}
