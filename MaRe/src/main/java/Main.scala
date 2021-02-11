import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {
  val conf = new SparkConf().setMaster("local").setAppName("Project")
  val sc = new SparkContext(conf)


  val rdd = sc.textFile("players_20.csv")
  val res = new MaRe(rdd)
    .map(
      inputMountPoint = TextFile("/sql"),
      outputMountPoint = TextFile("/out"),
      imageName = "sql",
      command = "java -jar sql.jar > out")
    .rdd.collect()

  res.foreach(println(_))


  /*val res = new MaRe(rdd)
    .map(
      inputMountPoint = TextFile("/dna"),
      outputMountPoint = TextFile("/count.txt"),
      imageName = "mllib",
      command = "java -jar mllib.jar > count.txt")
    reduce(
      inputMountPoint = TextFile("/counts"),
      outputMountPoint = TextFile("/sum"),
      imageName = "ubuntu",
      command = "awk '{s+=$1} END {print s}' /counts > /sum")
    .rdd.collect()*/

  /*val res = new MaRe(rdd)
    .map(
      inputMountPoint = TextFile("/dna"),
      outputMountPoint = TextFile("/count.txt"),
      imageName = "mllib",
      command = "java -cp mllib.jar com.yavuzozguven.Classification > count.txt")
    .rdd.collect()*/


  /*val res = new MaRe(rdd)
    .map(
      inputMountPoint = TextFile("/dna"),
      outputMountPoint = TextFile("/count.txt"),
      imageName = "graphx",
      command = "java -cp project.jar com.yavuzozguven.Network > count.txt")
    .rdd.collect()

  res.foreach(println(_))*/

  /*val res = new MaRe(rdd)
    .map(
      inputMountPoint = TextFile("/dna"),
      outputMountPoint = TextFile("/count.txt"),
      imageName = "anomaly",
      command = "java -jar project.jar > count.txt")
    .rdd.collect()

  res.foreach(println(_))*/
}
