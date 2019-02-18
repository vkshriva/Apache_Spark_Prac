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
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Database;

import static org.apache.spark.sql.functions.*;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\Users\\shrva02\\Documents\\software\\winutils-extra\\hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("SParkFullSqlSyntax").master("local[*]")
				.config("spark.sql.warehouse.dir", "file///C:/Users/shrva02/Documents/ApacheSpark Prac/tmp")
				.getOrCreate();

		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		/*
		 * If u want to work exactly as sql. First of all u need a table name 
		 */
		dataset.createOrReplaceTempView("student_name");
		
		//dataset.show();
		
		/*
		 * Now u can use any syntax of sql
		 */
		Dataset<Row> datasetNew =spark.sql("select * from student_name where subject='German'");
		
		datasetNew.show();
		/*
		 * Now u can write sql query 
		 */
		//spark.sql("")

		spark.close();
	}

}
