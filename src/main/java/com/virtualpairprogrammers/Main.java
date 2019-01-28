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
	   JavaRDD<Integer> myRdd =   sc.parallelize(inputData);

	    JavaRDD<Integer>  mapRDD = myRdd.map(value->1);
	    
	    Integer count =mapRDD.reduce((val1,val2) -> val1+val2);
	    
	    System.out.println(count);
	    
	    sc.close();
	}

}
