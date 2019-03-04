package com.virtualpairprogrammers.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class ApacheStreaming {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		System.setProperty("hadoop.home.dir", "C:\\Users\\shrva02\\Documents\\software\\winutils-extra\\hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

		SparkConf sConf = new SparkConf().setAppName("SparkStreamingWithAggregation").setMaster("local[*]");

		/*
		 * Batch Duration is set up here ie 2 sesconds
		 */

		JavaStreamingContext jsc = new JavaStreamingContext(sConf, Durations.seconds(2));

		/*
		 * To Use it u have to First run LoggingServer.java
		 */
		JavaReceiverInputDStream<String> initialData = jsc.socketTextStream("localhost", 8989);

		JavaDStream<String> result = initialData.map(text -> text);
		/*
		 * Lets Count the number of levels
		 */

		JavaPairDStream<String, Integer> pairDStream = result
				.mapToPair(rawData -> new Tuple2<>(rawData.split(",")[0], 1));

		JavaPairDStream<String, Integer> pairDStream2 = pairDStream.reduceByKey((x, y) -> x + y);

		//pairDStream2.print();

		/*
		 * But here You will Find that aagregation has been done in current RDD .We eare
		 * not storing history data So we can say that It is picking up the data from
		 * last Batch.
		 */

		/*
		 * Now wif you want to aggreage with last batch as well .Then u have to help of
		 * Window. By default all aggregation method will work on latest batch .But
		 * Every Aggregation has another version of method where we set Window Duration
		 * just like we set batch duration
		 *  JavaStreamingContext jsc = new JavaStreamingContext(sConf, Durations.seconds(2));
		 */
		
		/*
		 * Now it will take all the batch from last 2 mins 
		 */
		pairDStream =pairDStream.reduceByKeyAndWindow((x, y) -> x + y,Durations.minutes(2));
		
		pairDStream.print();

		jsc.start();
		jsc.awaitTermination();
	}

}
