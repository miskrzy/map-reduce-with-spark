import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object MyMain {
  def calcNodes(sc: SparkContext, path:String): Long ={
    val lines = sc.textFile(path).filter(!_.contains("#"))
    val nodes = lines.flatMap(elem =>{
      val edge = elem.split("\\s")
      List(edge(0).toInt->1,edge(1).toInt->1)
    }).reduceByKey(_+_)

    nodes.count()
  }

  def task1_indeg(sc: SparkContext,path:String,nodes:Long): Unit ={

    val lines = sc.textFile(path).filter(!_.contains("#"))
    val result = lines.map(elem => {
      val edge = elem.split("\\s")
      (edge(1).toInt,1)
    }).reduceByKey((a,b) => a+b)
    val average = result.map(a => (1,a._2)).reduce((a,b) => (1, a._2+b._2))._2/nodes.toDouble
    println(average)
  }

  def task1_outdeg(sc: SparkContext,path:String,nodes:Long): Unit ={

    val lines = sc.textFile(path).filter(!_.contains("#"))
    val result = lines.map(elem => {
      val edge = elem.split("\\s")
      (edge(0).toInt,1)
    }).reduceByKey((a,b) => a+b)

    val average = result.map(a => (1,a._2)).reduce((a,b) => (1, a._2+b._2))._2/nodes.toDouble
    println(average)
  }

  def task2(sc: SparkContext,path:String,nodes:Long): Unit ={
    val lines = sc.textFile(path).filter(!_.contains("#"))

    val undirectEdges = lines.map(elem => {
      val edge = elem.split("\\s")
      (Set(edge(0).toInt,edge(1).toInt),1)
    }).reduceByKey((a,b) => a+b).map(elem => {
      val edge = elem._1.toSeq
      (edge.head,edge.last)
    })

    val adjL = undirectEdges.flatMap(edge =>{
      List((edge._1, Set(edge._2)),(edge._2, Set(edge._1)))
    }).reduceByKey(_++_)

    val adjMmap = adjL.collect().toMap

    val result = adjL.flatMap(elem =>{
      for(x <- elem._2.toSeq) yield (elem._1, (elem._2 & adjMmap(x)).count(_=>true))
    }).reduceByKey(_+_).map(elem =>{
      val deg = adjMmap(elem._1).count(_=>true).toDouble
      (elem._1, if(deg>1) elem._2.toDouble/ deg/(deg-1.0) else 0.0, deg)
    })

    val average = result.map(a => (a._2,1.0)).reduce((a,b)=> {
      val current = a._2 + b._2
      ((a._1 * a._2 + b._1 * b._2)/current,current)
    })
    println(average._1)


  }
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c://hadoop//")
    val conf = new SparkConf().setAppName("bda_lab2").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val path = "C:\\Users\\User\\Desktop\\studia\\bda2\\big\\lab2\\src\\main\\resources\\web-Stanford.txt"
    val nodes = calcNodes(sc,path)

    task1_indeg(sc,path,nodes)
    task1_outdeg(sc,path,nodes)
    task2(sc,path,nodes)

  }
}
