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

		SparkSession spark = SparkSession.builder().appName("PivotObject").master("local[*]")
				.config("spark.sql.warehouse.dir", "file///C:/Users/shrva02/Documents/ApacheSpark Prac/tmp")
				.getOrCreate();

		Dataset<Row> testDataSet2 = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

		testDataSet2.createOrReplaceTempView("LogData");

		Dataset<Row> dataset = spark.sql("select level,date_format(datetime,'MMMM')as month  from LogData");

		// dataset.show();

		/*
		 * PivotTable : Pivot table is helpful when you wanted to do grouping with 2
		 * columns .Here once is level second is Month
		 * 
		 */

		Dataset<Row> pivotDataset = dataset.groupBy("level").pivot("month").count();

		pivotDataset.show(100);
		/*
		 * Now here Columns are not arranged in month order . In that case u can take
		 * help of List<Object> columns
		 */

		Object month[] = new Object[] { "January", "February", "March", "April", "May", "June", "July", "August",
				"September", "October", "November", "December" };
		List<Object> columns = Arrays.asList(month);

		Dataset<Row> pivotDatasetWithColumn = dataset.groupBy("level").pivot("month", columns).count();

		pivotDatasetWithColumn.show(100);

		/*
		 * In case any Month data is not available it will come as null
		 */

		Object month2[] = new Object[] { "January", "February", "March", "April", "May", "June", "July", "August",
				"September", "October", "November", "December", "DummyMonth" };
		List<Object> columns2 = Arrays.asList(month2);

		Dataset<Row> pivotDatasetWithColumn2 = dataset.groupBy("level").pivot("month", columns2).count();

		pivotDatasetWithColumn2.show(100);

		Dataset<Row> pivotDatasetWithColumn3 = dataset.groupBy("level").pivot("month", columns2).count().na().fill(0);

		pivotDatasetWithColumn3.show(100);

		spark.close();

	}

}
