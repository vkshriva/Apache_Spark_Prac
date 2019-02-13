package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.setProperty("hadoop.home.dir", "C:\\Users\\shrva02\\Documents\\software\\winutils-extra\\hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("Boring Exercise..").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> initialResource = sc.textFile("src/main/resources/subtitles/input.txt");

		/*
		 * Remove extra space and flatten the RDD
		 */
		JavaRDD<String> breakBySpace = initialResource.filter(data -> data.trim().length() > 0)
				.flatMap(data -> Arrays.asList(data.split(" ")).iterator());
   
		/*
		 * Only String is expected
		 */
		
		JavaRDD<String> sentence= breakBySpace.filter(data->data.matches("^[a-zA-Z]*$"));
		
		JavaPairRDD<String,Integer> countedPairRDD= sentence.filter(Util::isNotBoring)
		.mapToPair(word->new Tuple2<String,Integer>(word,1))
		.reduceByKey((val1,val2)->val1.intValue()+val2.intValue());		

	    /*
	     * Key as counted value
	     */
		
		countedPairRDD.mapToPair(tuple->new Tuple2<Integer,String>(tuple._2,tuple._1))
		.sortByKey(false)
		.collect().forEach(tuple -> { System.out.println(tuple._1 + ": " + tuple._2);});
		
		
		/*
		 * To run WEBUI u have to keep server/prog running so that if u hit localhost:4040  u can see Explain plan 
		 */
		
		Scanner kb = new Scanner(System.in);
		
     	  kb.next();

		sc.close();

	}

}
