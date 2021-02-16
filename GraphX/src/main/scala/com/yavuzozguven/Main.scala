package com.yavuzozguven

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Pass Network").setMaster("local[*]").set("spark.testing.memory", "471859200")
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("ERROR")

    val eventsData = sparkContext.textFile("event.csv")

    val sender: RDD[(VertexId, (String, String, String))] = eventsData.map(_.split(",")).map { arr =>
      if (arr(7).equals("Pass") && arr(13).toInt == 217 && !arr(79).equals("")) {
        (arr(73).toLong, (arr(74), arr(77), arr(78)))
      }
      else {
        null
      }
    }

    val recipient: RDD[(VertexId, (String, String, String))] = eventsData.map(_.split(",")).map { arr =>
      if (arr(7).equals("Pass") && arr(13).toInt == 217 && !arr(79).equals("")) {
        (arr(79).toLong, (arr(80), arr(85), arr(86)))
      }
      else {
        null
      }
    }

    val vertices = sender.filter(_ != null).union(recipient.filter(_ != null))

    val edges: RDD[Edge[String]] = eventsData.map(_.split(",")).map { arr =>
      if (arr(7).equals("Pass") && arr(13).toInt == 217 && !arr(79).equals("")) {
        Edge(arr(73).toLong, arr(79).toLong, "pass")
      }
      else {
        null
      }
    }


    val defaultUser = ("", "", "")
    val graph = Graph(vertices, edges.filter(_ != null), defaultUser)


    val rdd_x = sender.filter(_ != null).map { x =>
      (x._1.toDouble, x._2._2.toInt)
    }
    val agg_rdd_x = rdd_x.aggregateByKey((0, 0))((acc, value) => (acc._1 + value, acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val x = agg_rdd_x.mapValues(x => (x._1 / x._2))
    x.collect

    val rdd_y = sender.filter(_ != null).map { x =>
      (x._1.toDouble, x._2._3.toInt)
    }
    val agg_rdd_y = rdd_y.aggregateByKey((0, 0))((acc, value) => (acc._1 + value, acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val y = agg_rdd_y.mapValues(x => (x._1 / x._2))
    y.collect

    val names = sender.filter(_ != null).map { r =>
      (r._1.toDouble, r._2._1)
    }.distinct()

    val count: RDD[(Double, Long)] = sparkContext.parallelize(graph.triplets.map(_.srcId).countByValue().to).map(r => (r._1, r._2))


    val avg_pos = names.join(x.join(y)).join(count).collect().toList

    val netw = sparkContext.parallelize(graph.triplets.map { r =>
      (r.srcId.toDouble, r.dstId.toDouble)
    }.countByValue().to).map(r => (r._1, r._2)).collect().toList



    if(args(0).equals("avg")) {
      avg_pos.foreach(println(_))
    }
    if(args(0).equals("netw")){
      netw.foreach(println(_))
    }


    sparkContext.stop()
  }
}
