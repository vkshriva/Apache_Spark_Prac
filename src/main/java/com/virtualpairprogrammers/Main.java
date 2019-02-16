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

		/*
		 * SparkConf conf = new
		 * SparkConf().setAppName("startingSpark").setMaster("local[*]");
		 * JavaSparkContext sc = new JavaSparkContext(conf);
		 */

		SparkSession spark = SparkSession.builder().appName("SparkSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///C:/Users/shrva02/Documents/ApacheSpark Prac/tmp")
				.getOrCreate();

		/*
		 * Now Dataset will be for you as good as your Table. By deafault Dataset won't
		 * consider first row as header by using option("header",true) it will do sow
		 */
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

		/*
		 * It will show firsts 20 elements
		 */
		dataset.show();

		System.out.println(dataset.count());

		Row firstRow = dataset.first();

		String subject = firstRow.getString(2);

		// or

		String subject2 = firstRow.getAs("subject");

		System.out.println("subject: " + subject + "..subject2: " + subject2);

		/*
		 * Applying Filter U can write a where condition in filter
		 */

		Dataset<Row> filteredDataset = dataset.filter("subject='German' and score>54");

		/*
		 * You can write Filter with lambda expression .and filter will get row as input
		 */

		filteredDataset.show();

		Dataset<Row> filterDataSetUsingLambda = dataset.filter(row -> {
			return row.getAs("subject").equals("German") && Integer.parseInt(row.getAs("score")) > 54;
		});

		System.out.println("************Resulr using filter lambda expression***************");

		filterDataSetUsingLambda.show();

		System.out.println("**********Result using Column Filter*****************************");

		/*
		 * They are two ways to get Column object. Column object is nothing but
		 * representation of each column which makes ur code easy to read
		 */

		/*
		 * 1 way column filtering using Dataset.col()
		 */

		Column subjectCols = dataset.col("subject");
		Column scoreCols = dataset.col("score");
		/*
		 * Note below we are using equalTo() not equal method
		 */

		Dataset<Row> datasetUsngFilterColumnpart1 = dataset.filter(subjectCols.equalTo("German").and(scoreCols.gt(54)));

		datasetUsngFilterColumnpart1.show();

		/*
		 * 2 way column filtering using function class . Note function is class with
		 * small f in spark lib and it contains 2 static method col and column . To make
		 * it readable we can use static import and use any one of the method frm abv 2
		 * import static org.apache.spark.sql.functions.*;
		 */

		Dataset<Row> datasetUsngFilterColumnpart2 = dataset
				.filter(col("subject").equalTo("German").and(column("score").gt(54)));

		datasetUsngFilterColumnpart2.show();

		spark.close();
	}

}
