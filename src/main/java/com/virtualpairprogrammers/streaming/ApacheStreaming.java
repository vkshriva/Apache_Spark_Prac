package com.virtualpairprogrammers.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class ApacheStreaming {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.setProperty("hadoop.home.dir", "C:\\Users\\shrva02\\Documents\\software\\winutils-extra\\hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setAppName("DStreamApp").setMaster("local[*]");
		
		/* Duration will help to setup the duration for streaming 
		 * That is here every 30 sec with coming data RDD will be created 
		 * 
		 */
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
		
		JavaReceiverInputDStream<String> inputData = jsc.socketTextStream("localhost", 8989);
		
		/*
		 * This JavaDStream is as good as RDD . 
		 */
		JavaDStream<String> dStream = inputData.map(item->item);
		
		dStream.print();
		
		/*
		 * This will start SParKStreaming 
		 */
		jsc.start();
		
		
		 /*
		  * This will keep awake the SparkStreaming Jobs
		  */
		
		try {
			jsc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
