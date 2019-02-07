package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

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

		System.setProperty("hadoop.home.dir", "C:\\Users\\shrva02\\Documents\\software\\winutils-extra\\hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("Join example").setMaster("local[*]");
		JavaSparkContext sContext= new  JavaSparkContext(conf);
		
		List<Tuple2<Integer,Integer>> visits = new ArrayList<>();
		visits.add(new Tuple2<>(4,18));
		visits.add(new Tuple2<>(6,4));
		visits.add(new Tuple2<>(10,9));
		
		List<Tuple2<Integer,String>> users = new ArrayList<>();
		users.add(new Tuple2<>(1,"John"));
		users.add(new Tuple2<>(2,"Bob"));
		users.add(new Tuple2<>(3,"Desh"));
		users.add(new Tuple2<>(4,"Mash"));
		users.add(new Tuple2<>(5,"Tahs"));
		users.add(new Tuple2<>(6,"Sash"));
		
		JavaPairRDD<Integer, Integer> visitsData=sContext.parallelizePairs(visits);
		JavaPairRDD<Integer, String> userData=sContext.parallelizePairs(users);
		
		
		JavaPairRDD<Integer,Tuple2<Integer,String>> innerJoin= visitsData.join(userData);
				
		innerJoin.collect().forEach(System.out::println);
		
		
		 JavaPairRDD<Integer, Tuple2<Integer, org.apache.spark.api.java.Optional<String>>> leftJoin =visitsData.leftOuterJoin(userData);

		 leftJoin.collect().forEach(System.out::println);
		
		 System.out.println("*******************************");
		 JavaPairRDD<Integer, Tuple2<org.apache.spark.api.java.Optional<Integer>, String>> rightOuterJoin =visitsData.rightOuterJoin(userData);
		 
		 /*
		  * Traversing Optional Element in lame way .There will be some other way as well 
		  */
		 
		 rightOuterJoin.collect().forEach(tuple->{
			 System.out.println("UserID: "+tuple._1.intValue()+", No of visits: "+tuple._2._1.orElse(0)+", UserName is: "+tuple._2._2);
		 });
		 
		 System.out.println("***********************************");
	     rightOuterJoin.collect().forEach(System.out::println);
	}

}
