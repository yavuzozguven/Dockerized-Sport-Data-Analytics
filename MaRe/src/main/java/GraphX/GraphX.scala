package GraphX

import Mare.{MaRe, TextFile}
import org.apache.spark.{SparkConf, SparkContext}

object GraphX extends App {
  val conf = new SparkConf().setMaster("local").setAppName("Project")
  val sc = new SparkContext(conf)


  val rdd = sc.textFile("players_20.csv")

  val avg_pos = new MaRe(rdd)
    .map(
      inputMountPoint = TextFile("/input"),
      outputMountPoint = TextFile("/output"),
      imageName = "graphx",
      command = "java -jar project.jar avg > output")
    .rdd

  val netw = new MaRe(rdd)
    .map(
      inputMountPoint = TextFile("/input"),
      outputMountPoint = TextFile("/output"),
      imageName = "graphx",
      command = "java -jar project.jar netw > output")
    .rdd


  val avg_list: List[(Double, ((String, (Int, Int)), Long))] = avg_pos.map { r =>
    val toRemove = "()".toSet
    val words = r.filterNot(toRemove)
    val str = words.split(",")
    (str(0).toDouble, ((str(1).toString, (str(2).toInt, str(3).toInt)), str(4).toLong))
  }.collect().toList

  val netw_list: List[((Double, Double), Long)] = netw.map { r =>
    val toRemove = "()".toSet
    val words = r.filterNot(toRemove)
    val str = words.split(",")
    ((str(0).toDouble, str(1).toDouble), str(2).toLong)
  }.collect().toList


  Draw.network = netw_list
  Draw.data = avg_list
  Draw.top.visible = true

}
