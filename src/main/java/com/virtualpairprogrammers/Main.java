package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Wednesday 5 September 1632");
		inputData.add("ERROR: Friday 7 September 1854");
		inputData.add("WARN: Saturday 8 September 1942");

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("PairRDD").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> orignlRdd = sc.parallelize(inputData);

		/*
		 * Java PairRDD store data as key value pair is not similar to Map it allows
		 * duplicate key
		 */
		JavaPairRDD<String, String> pairRdd = orignlRdd.mapToPair(rawData -> {
			String content[] = rawData.split(":");
			return new Tuple2<>(content[0], content[1]);
		});

		JavaPairRDD<String, Long> modifyPairRdd = pairRdd.mapToPair(rawData -> {
			return new Tuple2<>(rawData._1, 1L);
		});

		JavaPairRDD<String, Long> resultPairRdd = modifyPairRdd.reduceByKey((val1, val2) -> val1 + val2);

		resultPairRdd.collect().forEach(rawData -> {
			System.out.println(rawData._1 + ": has " + rawData._2 + " lines");
		});

		System.out.println("******In One go****************************");

		sc.parallelize(inputData).mapToPair(rawdata -> new Tuple2<String, Long>(rawdata.split(":")[0], 1L))
				.reduceByKey((val1, val2) -> val1 + val2).collect().forEach(rawData -> {
					System.out.println(rawData._1 + ": has " + rawData._2 + " lines");
				});

		/*
		 * GroupBy can lead to severe perfommance problem .Avoid unless you are sure
		 * there is no better alternative
		 */
      System.out.println("**************Using Group By************");
		
		sc.close();

	}

}
