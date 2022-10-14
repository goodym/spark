package chap8

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS,LogisticRegressionModel}

object test_User {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf().setAppName("logistic").setMaster("spark://master:7077")
    conf.set("spark.executor.memory", "512m")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    if (args.length != 6) {
      println("output args")
      System.exit(1)
    }
    //设置需要的参数
    val inpath = args(0) //输入数据路径
    val model_logistic = args(1) //模型存储位置
    val f1Score_path = args(2) //F 值输出路径
    val threshold = args(3).toDouble //阈值
    val splitter = args(4) //数据分隔符
    val bili = args(5).toDouble //训练数据占比

    val data = sc.textFile(inpath).map { x =>
      val lines = x.split(splitter);
      LabeledPoint(lines(0).toDouble, Vectors.dense(lines.slice(1, lines.length).map(_.toDouble)))
    };
    //分割 training and test
    val splits = data.randomSplit(Array(bili, 1 - bili), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    //训练模型
    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training).
      setThreshold(threshold)

    val sameModel = LogisticRegressionModel.load(sc,model_logistic)
    val predictions = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (features, prediction)
    }
  }
}
