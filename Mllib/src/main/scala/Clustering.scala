import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

object Clustering{
  def main(args:Array[String]):Unit={
    val spark=new SparkSession.Builder().master("local[*]").appName("MLlib").getOrCreate()

    var df=spark.read.options(Map("sep"->",","header"->"true")).
      csv("players_20.csv")

    var dff=df.select("short_name","age","nationality","overall","potential","club","value_eur","wage_eur","international_reputation","gk_diving","gk_handling","gk_kicking","gk_positioning","gk_reflexes")
    dff=dff.na.drop

    dff=dff.withColumn("gk_score",((col("gk_diving")+col("gk_handling")+col("gk_kicking")+col("gk_positioning")+col("gk_reflexes"))/lit(5)))
    dff=dff.withColumn("overall_potential",((col("overall")+col("potential"))/lit(2)))

    val assembler=new VectorAssembler()
      .setInputCols(dff.select("gk_score","overall_potential").columns)
      .setOutputCol("features")

    dff=assembler.transform(dff)

    for(i<-2 to 10){
      val kmeans=new KMeans().setK(i).setSeed(1L)

      val model=kmeans.fit(dff)

      val predictions=model.transform(dff)

      val evaluator=new ClusteringEvaluator()

      val silhouette=evaluator.evaluate(predictions)

      //println(model.computeCost(dff))
    }

    val kmeans=new KMeans().setK(5).setSeed(1L)

    val model=kmeans.fit(dff)

    val predictions=model.transform(dff)

    predictions.groupBy("prediction").count().show

    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
  }
}
