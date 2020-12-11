import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.{BufferedReader, FileReader}

import org.apache.spark.rdd.RDD

import scala.collection.mutable

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
      (edge(0).toInt,1)
    }).reduceByKey((a,b) => a+b)
    //result.foreach(println)
    println(result.reduce((elem1,elem2) => (1, elem1._2 + elem2._2))._2 / nodes.toDouble)
    //println(result.count())
  }

  def task1_outdeg(sc: SparkContext,path:String,nodes:Long): Unit ={

    val lines = sc.textFile(path).filter(!_.contains("#"))
    val result = lines.map(elem => {
      val edge = elem.split("\\s")
      (edge(1).toInt,1)
    }).reduceByKey((a,b) => a+b)

    //result.foreach(println)
    println(result.reduce((elem1,elem2) => (1, elem1._2 + elem2._2))._2 / nodes.toDouble)
  }

  def task2(sc: SparkContext,path:String): Unit ={
    val lines = sc.textFile(path).filter(!_.contains("#"))

    val undirectEdges = lines.map(elem => {
      val edge = elem.split("\\s")
      (Set(edge(0).toInt,edge(1).toInt),1)
    }).reduceByKey((a,b) => a+b).map(elem => {
      val edge = elem._1.toSeq
      (edge.head,edge.last)
    })
    println(undirectEdges.count())




/*
    val undirect_x2 = sc.makeRDD(undirectEdges.map(edge => Set[(Int,Int)](edge._1 -> edge._2,edge._2 -> edge._1))
      .reduce((set1,set2) => set1 ++ set2).toSeq)

    undirect_x2.map(elem => {
      undirect_x2.filter(_._1==elem._2)
    })

*/
    //TRY FLATMAP


/*//map of node and its neighbours
    val nodeNeigh = undirect_x2.groupByKey()

    nodeNeigh(nodeNeigh.first()._2.head)

 */
  }
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c://hadoop//")
    val conf = new SparkConf().setAppName("bda_lab2").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val path = "C:\\Users\\User\\Desktop\\studia\\bda2\\big\\lab2\\src\\main\\resources\\web-Stanford.txt"
    //val nodes = calcNodes(sc,path)

    //task1_indeg(sc,path,nodes)
    //task1_outdeg(sc,path,nodes)
    task2(sc,path)

  }
}
