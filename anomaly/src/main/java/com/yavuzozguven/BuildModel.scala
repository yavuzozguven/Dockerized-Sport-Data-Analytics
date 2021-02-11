package com.yavuzozguven

import com.linkedin.relevance.isolationforest.IsolationForest
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}

object BuildModel {
  def main(args:Array[String]): Unit ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession.builder.master("local").appName("Project").getOrCreate

    val schema = new StructType()
      .add("time",DoubleType,true)
      .add("linaccel_x",DoubleType,true)
      .add("linaccel_y",DoubleType,true)
      .add("linaccel_z",DoubleType,true)
      .add("gravity_x",DoubleType,true)
      .add("gravity_y",DoubleType,true)
      .add("gravity_z",DoubleType,true)
      .add("gyro_x",DoubleType,true)
      .add("gyro_y",DoubleType,true)
      .add("gyro_z",DoubleType,true)
      .add("euler_x",DoubleType,true)
      .add("euler_y",DoubleType,true)
      .add("euler_z",DoubleType,true)
      .add("quaternion_w",DoubleType,true)
      .add("quaternion_y",DoubleType,true)
      .add("quaternion_x",DoubleType,true)
      .add("quaternion_z",DoubleType,true)


    val dff = spark.read.options(Map("sep"->",", "header"-> "true"))
      .schema(schema)
      .csv("/eSports_Sensors_Dataset-master/*/*/*/*.csv")


    val df = dff.drop("time")


    val ass = new VectorAssembler()
      .setInputCols(df.columns)
      .setOutputCol("features")

    val fDf = ass.setHandleInvalid("skip").transform(df)

    fDf.printSchema()
    fDf.show(truncate = false)

    val seed = 5043
    val Array(trnDta, tstDta) = fDf.randomSplit(Array(0.7,0.3),seed)

    val isd = new IsolationForest()
      .setNumEstimators(100)
      .setBootstrap(false)
      .setMaxSamples(256)
      .setFeaturesCol("features")
      .setPredictionCol("predicted_label")
      .setScoreCol("outlier_score")
      .setContamination(0.1)
      .setRandomSeed(1)

    val model = isd.fit(trnDta)

    model.write.overwrite().save("model")

  }

}
