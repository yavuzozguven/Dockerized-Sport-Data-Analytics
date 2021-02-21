import com.linkedin.relevance.isolationforest.IsolationForest
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StructType}

object BuildModel {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession.builder.master("local").appName("Project").getOrCreate

    val schema = new StructType()
      .add("time",DoubleType,false)
      .add("AF3/theta",DoubleType,false)
      .add("AF3/alpha",DoubleType,false)
      .add("AF3/betaL",DoubleType,false)
      .add("AF3/betaH",DoubleType,false)
      .add("AF3/gamma",DoubleType,false)
      .add("T7/theta",DoubleType,false)
      .add("T7/alpha",DoubleType,false)
      .add("T7/betaL",DoubleType,false)
      .add("T7/betaH",DoubleType,false)
      .add("T7/gamma",DoubleType,false)
      .add("Pz/theta",DoubleType,false)
      .add("Pz/alpha",DoubleType,false)
      .add("Pz/betaL",DoubleType,false)
      .add("Pz/betaH",DoubleType,false)
      .add("Pz/gamma",DoubleType,false)
      .add("T8/theta",DoubleType,false)
      .add("T8/alpha",DoubleType,false)
      .add("T8/betaL",DoubleType,false)
      .add("T8/betaH",DoubleType,false)
      .add("T8/gamma",DoubleType,false)
      .add("AF4/theta",DoubleType,false)
      .add("AF4/alpha",DoubleType,false)
      .add("AF4/betaL",DoubleType,false)
      .add("AF4/betaH",DoubleType,false)
      .add("AF4/gamma",DoubleType,false)


    val dff = spark.read.options(Map("sep" -> ",", "header" -> "true"))
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
    val Array(trnDta, tstDta) = fDf.randomSplit(Array(0.7, 0.3), seed)

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
