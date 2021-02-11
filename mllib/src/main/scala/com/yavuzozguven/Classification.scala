package com.yavuzozguven

import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression, RandomForestClassifier}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
object Classification {
    def main(args:Array[String]): Unit ={
      System.setProperty("hadoop.home.dir", "C:\\hadoop")
      val spark = SparkSession.builder().appName("tennis").master("local[*]").getOrCreate()

      var df = spark.read.options(Map("sep"->",", "header"-> "true")).
        csv("tennis.csv")

      val cat_fts = Seq("surface","winner_entry","loser_entry")

      cat_fts.foreach{r=>
        df = df.na.fill("null",Seq(r))
      }


      cat_fts.foreach{r=>
        val indexer =  new StringIndexer()
          .setInputCol(r)
          .setOutputCol(r+"Index")
          .fit(df)
        df = indexer.transform(df)
      }

      val num_fts = df.dtypes.filter (_._2 == "StringType") map (_._1)


      num_fts.foreach{r=>
        df = df.withColumn(r,df(r).cast("double"))
      }

      var df_winners = df.select("w_ace","w_df","w_svpt","w_1stIn"
        ,"w_1stWon","w_2ndWon","w_SvGms","w_bpSaved","w_bpFaced"
        ,"winner_seed","winner_age","winner_rank","winner_rank_points"
        ,"surfaceIndex","winner_entryIndex")
      var df_losers = df.select("l_ace","l_df","l_svpt"
        ,"l_1stIn","l_1stWon","l_2ndWon","l_SvGms","l_bpSaved"
        ,"l_bpFaced","loser_seed","loser_age","loser_rank","loser_rank_points"
        ,"surfaceIndex","loser_entryIndex")


      df_winners = df_winners.withColumn("label",lit(1))
      df_losers = df_losers.withColumn("label",lit(0))


      val cols = Seq("ace","df","svpt","1stIn","1stWon","2ndWon","SvGms","bpSaved","bpFaced","seed","age","rank","rank_points","surfaceIndex","entryIndex")


      for(i <- 0 until cols.size){
        df_winners = df_winners.withColumnRenamed(df_winners.columns(i),cols(i))
        df_losers = df_losers.withColumnRenamed(df_losers.columns(i),cols(i))
      }

      var df_final = df_winners.union(df_losers)

      df_final = df_final.na.fill(0)
      df_final = df_final.na.fill(0,Seq("seed"))

      val assembler = new VectorAssembler()
        .setInputCols(df_final.drop("label").columns)
        .setOutputCol("features")

      df_final = assembler.transform(df_final)

      val Array(training, test) = df_final.randomSplit(Array(0.7, 0.3), seed = 12345)




      val lr = new LogisticRegression()
        .setMaxIter(10)
        .setLabelCol("label")
        .setFeaturesCol("features")


      val lrModel = lr.fit(training)
      val lr_preds = lrModel.transform(test)

      val lr_rd = lr_preds.select("prediction","label").rdd.map(r=> (r.getDouble(0),r.getInt(1).toDouble))

      val metrics = new BinaryClassificationMetrics(lr_rd)

      val precision = metrics.precisionByThreshold()
      precision.collect().foreach(println(_ , "logistic regression precision"))

      val f1 = metrics.fMeasureByThreshold()
      f1.collect().foreach(println(_ , "logistic regression f1 score"))

      val recall = metrics.recallByThreshold()
      recall.collect().foreach(println(_ , "logistic regression recall score"))

      val auPRC = metrics.areaUnderPR()
      println(s"logistic regression area under precision-recall curve ${auPRC}")

      val auROC = metrics.areaUnderROC()
      println(s"logistic regression area under ROC ${auROC}")



      val lr_check = lr_preds.withColumn("correct",when(col("label").equalTo(col("prediction")),1).otherwise(0))

      lr_check.groupBy("correct").count.show

      val lr_df = lr_preds.select("label","prediction")
      lr_df.coalesce(1).write.option("header","true").option("sep",",").mode("overwrite").csv("outlr")



      val dt = new DecisionTreeClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")

      val dtModel = dt.fit(training)
      val dt_preds = dtModel.transform(test)

      val dt_rd = dt_preds.select("prediction","label").rdd.map(r=> (r.getDouble(0),r.getInt(1).toDouble))

      val metrics_dt = new BinaryClassificationMetrics(dt_rd)

      val precision_dt = metrics_dt.precisionByThreshold()
      precision_dt.collect().foreach(println(_ , "decision tree precision"))

      val f1_dt = metrics_dt.fMeasureByThreshold()
      f1_dt.collect().foreach(println(_ , "decision tree f1 score"))

      val recall_dt = metrics_dt.recallByThreshold()
      recall_dt.collect().foreach(println(_ , "decision tree recall score"))

      val auPRC_dt = metrics_dt.areaUnderPR()
      println(s"decision tree area under precision-recall curve ${auPRC_dt}")

      val auROC_dt = metrics_dt.areaUnderROC()
      println(s"decision tree area under ROC ${auROC_dt}")





      val dt_check = dt_preds.withColumn("correct",when(col("label").equalTo(col("prediction")),1).otherwise(0))

      dt_check.groupBy("correct").count.show




      val rf = new RandomForestClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setNumTrees(10)

      val rfModel = rf.fit(training)
      val rf_preds = rfModel.transform(test)

      val rf_rd = rf_preds.select("prediction","label").rdd.map(r=> (r.getDouble(0),r.getInt(1).toDouble))

      val metrics_rf = new BinaryClassificationMetrics(rf_rd)

      val precision_rf = metrics_rf.precisionByThreshold()
      precision_rf.collect().foreach(println(_ , "random forest precision"))

      val f1_rf = metrics_rf.fMeasureByThreshold()
      f1_rf.collect().foreach(println(_ , "random forest f1 score"))

      val recall_rf = metrics_rf.recallByThreshold()
      recall_rf.collect().foreach(println(_ , "random forest recall score"))

      val auPRC_rf = metrics_rf.areaUnderPR()
      println(s"random forest area under precision-recall curve ${auPRC_rf}")

      val auROC_rf = metrics_rf.areaUnderROC()
      println(s"random forest area under ROC ${auROC_rf}")







      val rf_check = rf_preds.withColumn("correct",when(col("label").equalTo(col("prediction")),1).otherwise(0))

      rf_check.groupBy("correct").count.show
    }
}
