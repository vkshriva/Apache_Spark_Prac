package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import parquet.org.apache.thrift.TUnion;
import scala.Tuple2;

/**
 * This class is used in the chapter late in the course where we analyse viewing
 * figures. You can ignore until then.
 */
public class ViewingFigures {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\Users\\shrva02\\Documents\\software\\winutils-extra\\hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = false;

		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		viewData = viewData.persist(StorageLevel.MEMORY_AND_DISK());
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		chapterData = chapterData.persist(StorageLevel.MEMORY_AND_DISK());
	    JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

		JavaPairRDD<Integer, Integer> numberOfChapterinCourseRDD = chapterData
				.mapToPair(tuple -> new Tuple2<Integer, Integer>(tuple._2, 1)).reduceByKey((val1, val2) -> val1 + val2);
		// .
		// numberOfChapterinCourseRDD.collect().forEach(System.out::println);
		// System.out.println("****************************************************");
		// TODO - over to you!

		JavaPairRDD<Integer, Integer> removeDuplicateviewData = viewData.distinct();

		// removeDuplicateviewData.collect().forEach(System.out::println);

		JavaPairRDD<Integer, Tuple2<Integer, Integer>> viewDataAndChapterDataJoin = removeDuplicateviewData
				.mapToPair(tuple -> new Tuple2<Integer, Integer>(tuple._2, tuple._1)).join(chapterData);

		JavaPairRDD<Integer, Integer> userMapWithCourseRDD = viewDataAndChapterDataJoin
				.mapToPair(tuple -> new Tuple2<>(tuple._2._1, tuple._2._2));

		JavaPairRDD<Tuple2<Integer, Integer>, Integer> intermediateCountUserCourse = userMapWithCourseRDD
				.mapToPair(tuple -> new Tuple2<>(new Tuple2<>(tuple._1, tuple._2), 1));

		// intermediateCountUserCourse.collect().forEach(System.out::println);

		JavaPairRDD<Tuple2<Integer, Integer>, Integer> countUserCourse = intermediateCountUserCourse
				.reduceByKey((val1, val2) -> val1 + val2);

		// countUserCourse.collect().forEach(System.out::println);

		JavaPairRDD<Integer, Integer> courseCountRDD = countUserCourse
				.mapToPair(tuple -> new Tuple2<>(tuple._1._2, tuple._2));
		// courseCountRDD.collect().forEach(System.out::println);

		JavaPairRDD<Integer, Tuple2<Integer, Integer>> naamkyalikhoonRDD = courseCountRDD
				.join(numberOfChapterinCourseRDD);
		// naamkyalikhoonRDD.collect().forEach(System.out::println);

		JavaPairRDD<Integer, Float> percentgRDD = naamkyalikhoonRDD.mapToPair(tuple -> {
			// int temp = (tuple._2._2 - tuple._2._1);
			float per = tuple._2._1 * 100 / tuple._2._2;
			return new Tuple2<>(tuple._1, per);

		});

		// percentgRDD.collect().forEach(System.out::println);

		JavaPairRDD<Integer, Integer> finalRDD = percentgRDD.mapToPair(tuple -> {
			int score = 0;
			int percentage = (int) (float) tuple._2;
			if (percentage > 90) {
				score = 10;
			} else if (percentage > 50) {
				score = 4;
			} else if (percentage > 25) {
				score = 2;
			}
			return new Tuple2<>(tuple._1, score);
		}).reduceByKey((val1, val2) -> val1.intValue() + val2.intValue()).sortByKey(false);

		//finalRDD.collect().forEach(System.out::println);
		
		JavaPairRDD<Integer,Tuple2<String,Integer>> mergeWithTitle = titlesData.join(finalRDD);
		//mergeWithTitle.collect().forEach(System.out::println);
		
		JavaPairRDD<String,Integer> finalDataWithName = mergeWithTitle.mapToPair(tuple->new Tuple2<>(tuple._2._1,tuple._2._2));
		finalDataWithName.collect().forEach(System.out::println);
		
		
		Scanner kb = new Scanner(System.in);
		
			kb.nextLine();	
		
		sc.close();
	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {

		if (testMode) {
			// (chapterId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("src/main/resources/viewing figures/titles.csv").mapToPair(commaSeparatedLine -> {
			String[] cols = commaSeparatedLine.split(",");
			return new Tuple2<Integer, String>(new Integer(cols[0]), cols[1]);
		});
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {

		if (testMode) {
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96, 1));
			rawChapterData.add(new Tuple2<>(97, 1));
			rawChapterData.add(new Tuple2<>(98, 1));
			rawChapterData.add(new Tuple2<>(99, 2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("src/main/resources/viewing figures/chapters.csv").mapToPair(commaSeparatedLine -> {
			String[] cols = commaSeparatedLine.split(",");
			return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
		});
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {

		if (testMode) {
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return sc.parallelizePairs(rawViewData);
		}

		return sc.textFile("src/main/resources/viewing figures/views-*.csv").mapToPair(commaSeparatedLine -> {
			String[] columns = commaSeparatedLine.split(",");
			return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
		});
	}
}
