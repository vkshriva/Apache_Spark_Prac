package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

	/*
	 * FlatMap ->Given a single value 0 or more output is given Map->One value
	 * exactly 1 output
	 */

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*
		 * Split all the sentences into a single words
		 */
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Wednesday 5 September 1632");
		inputData.add("ERROR: Friday 7 September 1854");
		inputData.add("WARN: Saturday 8 September 1942");

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("FlatMapAndFilter").setMaster("local[*]");
		JavaSparkContext sctxt = new JavaSparkContext(conf);

		/*
		 * Since FlatMap return Iterator<Object> Thus converting String[] to list and
		 * call Iterator method
		 */

		sctxt.parallelize(inputData).flatMap(rawdata -> Arrays.asList(rawdata.split(" ")).iterator()).collect()
				.forEach(System.out::println);
		
		System.out.println("*********************Using Filter********************");

		/*
		 * Filter is used to filter some data .Collect only thode whose word is greater than 1
		 */
		sctxt.parallelize(inputData).flatMap(rawdata -> Arrays.asList(rawdata.split(" ")).iterator())
				.filter(word -> word.length() > 1).collect().forEach(System.out::println);
      
		sctxt.close();
	}

}
