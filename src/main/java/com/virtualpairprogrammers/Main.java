package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		List<Integer> inputData = new ArrayList<>();
		inputData.add(13);
		inputData.add(28);
		inputData.add(36);
		inputData.add(49);

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("Jai Shri Krishna").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Integer> myRdd = sc.parallelize(inputData);

		/*
		 * While uncommenting this you "may or may not " get NotSerializableException
		 * myRdd.foreach(System.out::println);
		 * 
		 * But definately it will throws in distributed environmnt Reason : Since
		 * internally there is chances that ur system contains multiple cpu and in that
		 * case whatever function we are passing to RDD it has to be distributed thus
		 * Spark will expect that to be; implememted Seriaizable
		 * 
		 * thus to avoid this error we will take help of collect() which collect all the
		 * RDD data in one collection
		 */

		myRdd.collect().forEach(System.out::println);

		JavaRDD<Double> sqrtRdd = myRdd.map(value -> Math.sqrt(value));

		sc.close();

	}

}
