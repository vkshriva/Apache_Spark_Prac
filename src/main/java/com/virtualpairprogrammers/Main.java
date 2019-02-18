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

		SparkSession spark = SparkSession.builder().appName("Storingjavaobject").master("local[*]")
				.config("spark.sql.warehouse.dir", "file///C:/Users/shrva02/Documents/ApacheSpark Prac/tmp")
				.getOrCreate();
		/*
		 * Here u are using java object as SparkSql Object .for this List will be create
		 * of row Type which has been created using RowFactory
		 */
		List<Row> inMemory = new ArrayList<Row>();
		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
		inMemory.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));
		
		/*
		 * Defining each column name ,datatype and metadata 
		 */
		StructField[] fields = new StructField[] {
				new StructField("level",DataTypes.StringType,false, Metadata.empty()),
				new StructField("date",DataTypes.StringType,false, Metadata.empty())
		};
		
		StructType schema = new StructType(fields );
		Dataset<Row>testDataSet =spark.createDataFrame(inMemory, schema );
		
		testDataSet.show();

	}

}
