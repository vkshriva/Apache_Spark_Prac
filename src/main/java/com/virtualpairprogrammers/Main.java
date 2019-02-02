package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
		 * flatten each sentence
		 */
		JavaRDD<String> sentences = initialResource.flatMap(rawData -> Arrays.asList(rawData.split(" ")).iterator());
		
		
	/*	
		 * Removes integer and special character . Only String will be there in RDD. no integer 
		 
		JavaRDD<String>sentencesOnlyWord = sentences.filter((sentence->{
		    Pattern pattern = Pattern.compile("[a-zA-Z]");
		    pattern.matches(pattern, sentence);
			
		}));*/
		
		
		
		sentences.filter(Util::isNotBoring)
				.mapToPair(word -> new Tuple2<String, Long>(word, 1L))
				.reduceByKey((val, val2) -> val.longValue() + val2.longValue())
				.mapToPair(tuple->new Tuple2<Long,String>(tuple._2,tuple._1))
				.sortByKey(false)
				.collect().forEach(tuple -> {
					System.out.println(tuple._1 + ": " + tuple._2);
				});
		 /* .collect() .forEach(System.out::println);*/
		 

		sc.close();

	}

}
