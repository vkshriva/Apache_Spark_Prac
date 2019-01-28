package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

	/*Let there is scenario where you want to keep inputData  and its corresponding sqrt in 1 RDD
	 * 1 Solution will be create your class and push the data as below
	 * 
	 * JavaRDD<MyClass> rdd = myRdd.map(value-> new MyClass(value,Math.sqrt(value)))
	 * 
	 * But Instead of this you can use Tupple of Scala wheras in Java it gives you some Class to deal with 
	 * it 
	 * Tuple class is available from Tuple2 to Tuple22 based on number 	 */
	
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
		JavaRDD<Integer> actualRdd = sc.parallelize(inputData);
		
		
		JavaRDD<Tuple2<Integer,Double>> combinedRdd = actualRdd.map(value-> new Tuple2<>(value,Math.sqrt(value)));
        
		
		
		sc.close();

	}

}
