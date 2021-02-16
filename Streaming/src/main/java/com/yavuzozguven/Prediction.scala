package com.yavuzozguven

import com.linkedin.relevance.isolationforest.IsolationForestModel
import net.minidev.json.JSONObject
import org.apache.pulsar.client.api.PulsarClient
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.parsing.json.JSON

object Prediction {
  def main(args:Array[String]): Unit ={
    val spark = SparkSession.builder.master("local").appName("Predict").getOrCreate
    val client = PulsarClient.builder()
      .serviceUrl("pulsar://40.114.67.6:6650")
      .build()

    val sq = client.newConsumer()
      .topic("test-first")
      .subscriptionName("subscription")
      .subscribe()

    val isoModel = IsolationForestModel.load("model")
    while(true){
      val msg = sq.receive()
      var str_sq = new String(msg.getData)

      if(str_sq.equals("end")) {
        sq.acknowledge(msg)
        System.exit(0)
      }


      str_sq = str_sq.dropRight(1)
      str_sq = str_sq.drop(1)

      val Data = Seq(
        Row(str_sq)
      )

      val Schema = List(
        StructField("features", StringType, true)
      )

      val someDF = spark.createDataFrame(
        spark.sparkContext.parallelize(Data),
        StructType(Schema)
      )

      val parse = udf((x:String) => Vectors.parse(x).asML)
      val df = someDF.withColumn("features",parse(col("features")))


      val res = isoModel.transform(df).filter("predicted_label > 0")
      if(res.count()>0)
        res.select("outlier_score","predicted_label").show()


      sq.acknowledge(msg)
    }
  }

}
