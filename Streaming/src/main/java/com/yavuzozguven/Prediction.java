package com.yavuzozguven;

import java.nio.charset.StandardCharsets;
import java.util.*;
import com.linkedin.relevance.isolationforest.IsolationForestModel;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.spark.SparkStreamingPulsarReceiver;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class Prediction {


    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming");

        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(60));

        ConsumerConfigurationData<byte[]> pulsarConf = new ConsumerConfigurationData();

        IsolationForestModel model = IsolationForestModel.load("model");

        String inputTopic = "test-first";
        String subscription = "subscription";
        String serviceUrl = "pulsar://<ip_address>:6650/";


        Set<String> set = new HashSet<>();
        set.add(inputTopic);
        pulsarConf.setTopicNames(set);
        pulsarConf.setSubscriptionName(subscription);

        SparkStreamingPulsarReceiver pulsarReceiver = new SparkStreamingPulsarReceiver(
                serviceUrl,
                pulsarConf,
                new AuthenticationDisabled());

        JavaReceiverInputDStream<byte[]> lineDStream = jsc.receiverStream(pulsarReceiver);
        JavaPairDStream<String, Integer> result = lineDStream.flatMap(x -> {
            String line = new String(x, StandardCharsets.UTF_8);
            List<String> list = Arrays.asList(line.split(" "));
            return list.iterator();
        })
                .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
                .reduceByKey((x, y) -> x + y);

        result.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
                JavaRDD<Row> rowRDD = stringIntegerJavaPairRDD.map(new Function<Tuple2<String, Integer>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        Row row;
                        try{
                            row = RowFactory.create(Vectors.parse(stringIntegerTuple2._1).asML());
                        }
                        catch (Exception e){
                            row = RowFactory.create(stringIntegerTuple2._1);
                        }
                        return row;
                    }
                });
                StructType schema = DataTypes.createStructType(new StructField[] {DataTypes.createStructField("features", new VectorUDT(), true)});
                //Get Spark session
                SparkSession spark = JavaSparkSessionSingleton.getInstance(stringIntegerJavaPairRDD.context().getConf());
                Dataset<Row> msgDataFrame = spark.createDataFrame(rowRDD, schema);
                msgDataFrame = msgDataFrame.na().drop();

                Dataset<Row> res = model.transform(msgDataFrame).filter("predicted_label > 0");


                res.select("outlier_score","predicted_label").show();
            }
        });




        jsc.start();
        jsc.awaitTerminationOrTimeout(60*1000);
    }
}
 class JavaSparkSessionSingleton {
     private static transient SparkSession instance = null;
     public static SparkSession getInstance(SparkConf sparkConf) {
         if (instance == null) {
             instance = SparkSession
                     .builder()
                     .config(sparkConf)
                     .getOrCreate();
         }
         return instance;
     }
 }
