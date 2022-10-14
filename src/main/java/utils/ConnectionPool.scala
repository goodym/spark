package utils
import java.sql.{Connection, DriverManager}
import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import org.slf4j.LoggerFactory

object ConnectionPool {
  val logger=LoggerFactory.getLogger(this.getClass())
  private val pool={
    try{
      Class.forName("com.mysql.jdbc.Driver")
      //  DriverManager.getConnection("jdbc:mysql://home.hddly.cn:53306/test?useSSL=false","test","test")
      val config = new BoneCPConfig()
      config.setUsername("test")
      config.setPassword("test")
      config.setJdbcUrl("jdbc:mysql://home.hddly.cn:53306/test")
      config.setMinConnectionsPerPartition(2) //最小连接数
      config.setMaxConnectionsPerPartition(5) //最大连接数
      config.setCloseConnectionWatch(true)  //关闭的时候要不要监控
      Some(new BoneCP(config))

    }catch {
      case e:Exception=>{
        e.printStackTrace()
        None
      }
    }
  }
  def getConnection():Option[Connection]={
    pool match {
      case  Some(pool)=> Some(pool.getConnection)
      case None=>None
    }
  }
  def  returnConnection(connection:Connection)={
    if(null != connection){
      connection.close() //这个地方不能关闭，应该要返回到池里面去吃才行
    }
  }

}
