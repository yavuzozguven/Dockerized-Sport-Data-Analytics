package com.yavuzozguven

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{MinMaxScaler, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{DecisionTreeRegressor, LinearRegression, RandomForestRegressor}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}

object Regression {
  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder().master("local[*]").appName("MLlib").getOrCreate()

    var df = spark.read.options(Map("sep" -> ",", "header" -> "true")).
      csv("players_20.csv")

    var df_cont = df.select("age", "height_cm", "weight_kg", "overall", "potential", "wage_eur", "international_reputation", "weak_foot", "skill_moves", "release_clause_eur",
      "pace", "shooting", "passing", "dribbling", "defending", "physic", "gk_diving", "gk_handling", "gk_kicking", "gk_reflexes", "gk_speed", "gk_positioning",
      "attacking_crossing", "attacking_finishing", "attacking_heading_accuracy", "attacking_short_passing", "attacking_volleys",
      "skill_dribbling", "skill_curve", "skill_fk_accuracy", "skill_long_passing", "skill_ball_control", "movement_acceleration", "movement_sprint_speed",
      "movement_agility", "movement_reactions", "movement_balance", "power_shot_power", "power_jumping", "power_stamina", "power_strength", "power_long_shots",
      "mentality_aggression", "mentality_interceptions", "mentality_positioning", "mentality_vision", "mentality_penalties", "mentality_composure",
      "defending_marking", "defending_standing_tackle", "defending_sliding_tackle", "goalkeeping_diving", "goalkeeping_handling", "goalkeeping_kicking",
      "goalkeeping_positioning", "goalkeeping_reflexes")

    var df_cat = df.select("preferred_foot", "work_rate", "body_type", "team_position", "nation_position", "player_traits")

    val cat_vals = df_cat.columns
    df_cat = df_cat.na.fill("na")


    val indexers = df_cat.columns.map(column => {
      val indexer = new StringIndexer()
      indexer.setInputCol(column)
      indexer.setOutputCol(column + "_index")
    })

    val pipe = new Pipeline()
    pipe.setStages(indexers)
    df_cat = pipe.fit(df_cat).transform(df_cat)

    cat_vals.foreach { r =>
      df_cat = df_cat.drop(r)
    }

    //Merge dataframes
    val df1 = df_cat.withColumn("row_id", monotonically_increasing_id())

    val df2 = df_cont.withColumn("row_id", monotonically_increasing_id())

    var df_merged = df1.join(df2, ("row_id")).drop("row_id")

    df_cont.columns.foreach { r =>
      df_merged = df_merged.withColumn(r, df_merged(r).cast("double"))
    }
    df_merged = df_merged.na.fill(0)



    val assembler = new VectorAssembler()
      .setInputCols(df_merged.columns)
      .setOutputCol("features")

    df_merged = assembler.transform(df_merged)

    df_merged.columns.foreach { r =>
      if (!r.equals("features"))
        df_merged = df_merged.drop(r)
    }


    val df3 = df_merged.withColumn("row_id", monotonically_increasing_id())

    val df4 = df.select(col("value_eur").as("label").cast("long")).withColumn("row_id", monotonically_increasing_id())

    var df_final = df3.join(df4, ("row_id")).drop("row_id")

    df_final = df_final.where("label > 150000 and label < 200000")

    val Array(trainingData, testData) = df_final.randomSplit(Array(0.7, 0.3))



    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFeaturesCol("features")

    val lrModel = lr.fit(trainingData)

    val predictions = lrModel.transform(testData)
    predictions.select("prediction", "label", "features").show(10)

    val trainingSummary = lrModel.summary
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")





    val dt = new DecisionTreeRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val modelDt = dt.fit(trainingData)

    val predictionsDt = modelDt.transform(testData)
    predictionsDt.select("prediction", "label", "features").show(10)


    val evaluatorDT = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("r2")
    val r2DT = evaluatorDT.evaluate(predictionsDt)
    val rmseDT = evaluatorDT.setMetricName("mae").evaluate(predictionsDt)

    println(s"Root Mean Squared Error (RMSE) on test data = $rmseDT")
    println(s"R2 Score on test data = $r2DT")




    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val modelRf = rf.fit(trainingData)

    val predictionsRf = modelRf.transform(testData)
    predictionsRf.select("prediction", "label", "features").show(10)

    val evaluatorRF = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("r2")
    val r2RF = evaluatorRF.evaluate(predictionsRf)
    val rmseRF = evaluatorRF.setMetricName("rmse").evaluate(predictionsDt)

    println(s"Root Mean Squared Error (RMSE) on test data = $rmseRF")
    println(s"R2 Score on test data = $r2RF")


    predictionsDt.printSchema()
    val rd = predictionsDt.select("prediction","label").rdd.map(r=> (r.getDouble(0),r.getLong(1).toDouble))


    val metrics = new RegressionMetrics(rd)
    println(s"r2 = ${metrics.r2}")
    println(s"rmse = ${metrics.rootMeanSquaredError}")


    trainingData.describe().show()

  }
}
