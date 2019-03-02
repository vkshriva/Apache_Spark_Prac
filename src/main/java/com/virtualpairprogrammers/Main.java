package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalog.Database;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\Users\\shrva02\\Documents\\software\\winutils-extra\\hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("MoreOnAggregation").master("local[*]")
				.config("spark.sql.warehouse.dir", "file///C:/Users/shrva02/Documents/ApacheSpark Prac/tmp")
				.getOrCreate();

		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
       
	    /*
	     * Let say u want to create a column .ie pass column where only A+ student will get pass mark.
	     * lit is method of functionals class and its meaning is literal .
	     * BUt it won't work for complex requiremnt
	     * 
	     * withColumn helps to add new column firs t parameter will take colName and another one literal
	     * 
	     */
		
		Dataset<Row> datasetUsingLiteral = dataset.withColumn("pass_status", lit(col("grade").equalTo("A+")));
		datasetUsingLiteral.show();
		
		/* Using udf(user defined Function)
		 * 
		 */
        
		spark.udf().register("myUDF",(grade)->grade.equals("A+"),DataTypes.BooleanType);
		
		Dataset<Row> datasetUsingLambdaAndUDF = dataset.withColumn("pass_Status", callUDF("myUDF", col("grade")));
		
		datasetUsingLambdaAndUDF.show();
		
		spark.close(); 

	}

}
