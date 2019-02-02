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

		JavaRDD<String> tempResource = initialResource.map(text -> text.replaceAll("[^a-zA-z\\s]", "").toLowerCase());

		JavaRDD<String> sentences = tempResource.flatMap(rawData -> Arrays.asList(rawData.split(" ")).iterator());

		JavaRDD<String> removeblankLines = sentences.filter(sentenc -> sentenc.trim().length() > 0);

		JavaPairRDD<Long, String> finalAnswer = removeblankLines.filter(Util::isNotBoring)
				.mapToPair(word -> new Tuple2<String, Long>(word, 1L))
				.reduceByKey((val, val2) -> val.longValue() + val2.longValue())
				.mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1)).sortByKey(false);

		/*
		 * Issue with foreach() not forEach
		 * 
		 * finalAnswer.foreach(tuple -> { System.out.println(tuple); });
		 * 
		 * The out will not be in sorted order.Actually it doesn't mean it is not sorted
		 * .But it is exceptional with foreach()
		 * 
		 * Reason is -> Some says becoz our data is in partition thus it will be sorted
		 * in within partion But actually it is beco of multhithreading .foreach runs in
		 * multiple thread mode and it is not guarantee which thread will print data.
		 * Thus it will show u not sorted form This issue will occurs only with foreach.
		 * 
		 * To solve this issue one solution can be Coalesc
		 * 
		 * Coalesc is the way to reduce the partition ,Here u make partiotion to only 1
		 * .As a result u will get correct value in foreach.But again it is risky as if
		 * data is more u will get outof memory exception
		 * 
		 * or u can use collect()
		 * 
		 * or take()
		 */

		/*
		 * How to know number o partion is done by ApacheSpark
		 */
		int partion = finalAnswer.getNumPartitions(); // Answer is 2
		System.out.println("Partition is " + partion);

		finalAnswer = finalAnswer.coalesce(1);

		System.out.println("Partition after coalesc is " + finalAnswer.getNumPartitions());

		/*
		 * take(number of element u want from top)
		 */

		List<Tuple2<Long, String>> ans = finalAnswer.take(10); // gives top 10 element

		ans.forEach(System.out::println);

		System.out.println("*******************************************");

		finalAnswer.collect().forEach(tuple -> {
			System.out.println(tuple);
		});

		sc.close();

	}

}
