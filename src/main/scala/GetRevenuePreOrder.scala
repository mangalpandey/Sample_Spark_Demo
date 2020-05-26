import org.apache.spark.{SparkConf, SparkContext}
object GetRevenuePreOrder{
  def main(args: Array[String]) : Unit = {
    var conf = new SparkConf().
      setAppName("MyApplication").
      setMaster("local")
    var sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val orderItem = sc.textFile("hdfs://localhost:8020/user/data/part-00000")
    var revenuePreOrder = orderItem.
      map(oi=> (oi.split(",")(1).toInt, oi.split(",")(4).toFloat)).
      reduceByKey((x,y)  => (x+y)).
      map(oi=> oi._1 + "," + oi._2)
    revenuePreOrder.collect.foreach(println)


  }
}