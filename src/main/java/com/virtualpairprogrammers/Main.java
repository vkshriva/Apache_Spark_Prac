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

		dataset.show();

		/*
		 * if u use it it will complain exception tht score is not numberic type By
		 * Default every column is string Dataset<Row> aggregation1 =
		 * dataset.groupBy("subject").max("score");
		 * 
		 */

		Column scoreCols = dataset.col("score");
		/*
		 * It should work but for now it is noot working . May be later version it will
		 * start Dataset<Row> aggregation1 =
		 * dataset.groupBy(scoreCols.cast(DataTypes.IntegerType));
		 */

		/*
		 * RelationGroupDataset gives u agg() which can be used for aggregation
		 */

		Dataset<Row> aggregation1 = dataset.groupBy(functions.col("subject"))
				.agg(functions.max(functions.col("score").cast(DataTypes.IntegerType)));
		aggregation1.show();

		/*
		 * Using functions class with static import and agg can except any number of
		 * expression so adding minimum score as well
		 * 
		 */

		aggregation1 = dataset.groupBy(col("subject")).agg(
				max(col("score").cast(DataTypes.IntegerType)).alias("MaxNumber"),
				min(col("score").cast(DataTypes.IntegerType)).alias("MinNumber"));

		aggregation1.show();

		spark.close();

	}

}
