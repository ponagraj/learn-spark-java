package com.learning.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * Word Count Example of Spark Java
 *
 */
public class WordCount 
{
    public static void main( String[] args )
    {
    	//System.setProperty("hadoop.home.dir", "F:\\winutil"); //to be modified based on winutils.exe 
    	SparkConf conf = new SparkConf()
    			.setAppName("Sample Learning")
    			.setMaster("local[*]");
    	SparkContext sc = new SparkContext(conf);
    	SparkSession session = new SparkSession(sc);
    	JavaSparkContext jsc = new JavaSparkContext(sc);
    	//JavaRDD<String> df = jsc.textFile("F:\\eclipse-workspace\\testfiles\\wordcount.txt",4);
    	JavaRDD<String> df = jsc.textFile("src\\main\\res\\wordcount.txt",4);
    	System.out.println("start count is: "+df.count());
    	JavaRDD<String> flatMapped = df.flatMap(new FlatMapFunction<String, String>() {
    		public Iterator<String> call(String arg0) throws Exception {
    			return Arrays.asList(arg0.split(" ")).iterator();
    		}
		});
    	System.out.println("flatmap count count: " +flatMapped.count());
    	JavaPairRDD<String, Integer> pairRdd = flatMapped.mapToPair(new PairFunction<String, String, Integer>() {
    		public Tuple2<String, Integer> call(String arg0) throws Exception {
    			return new Tuple2(arg0,1);
    		}
		});
    	JavaPairRDD<String, Integer> reducedRdd = pairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0+arg1;
			}
		});
    	System.out.println("collected data: " +reducedRdd.collect());
    	reducedRdd.count();
    }
}
