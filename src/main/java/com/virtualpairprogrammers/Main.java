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
	   
	   
	   /*
	    * local[*] => local means we are using Spark in local not in cluster
	    *   [*] indicates that we don't know and use as many cores available for thread   
	    * 
	    * 
	    */
	   SparkConf conf = new SparkConf().setAppName("Jai Shri Krishna").setMaster("local[*]");
	   
	  /* if you just insert it means u are using only single thread 
	   * SparkConf conf = new SparkConf().setAppName("Jai Shri Krishna").setMaster("local");*/
	   
	   
	   /*
	    * Represent a connection to Spark Cluster. This object allows to communicate the Spark
	    */
	   JavaSparkContext sc = new JavaSparkContext(conf);
	   
	   /*
	    * Now Load some kind some data in Spark 
	    * 
	    * parrallelize-> Load a java collection
	    */
		
	    /*
	     * JavaRDD impleneted in Scala itself and it is bridge between Scala and 
	     */
	    JavaRDD<Integer> myRdd =   sc.parallelize(inputData);
	   // myRdd.foreach(System.out::println);
	    
	    /*
	     * Mapper
	     */
	    JavaRDD<Double> sqrtRdd = myRdd.map(value->Math.sqrt(value)); 
	    
	    /*
	     * Reducer
	     */
	    Double result = sqrtRdd.reduce((v1,v2)->v1+v2); 
	    
	    /*
	     * Printing the RDD contents
	     * Not Recommended
	     */
	    sqrtRdd.foreach(value->System.out.println(value));
	    
	    /*
	     * Counting the RDD
	     */
	    Long numberOfElement =sqrtRdd.count();

	    System.out.println(numberOfElement );
	    
	    sc.close();
	   
	}

}
