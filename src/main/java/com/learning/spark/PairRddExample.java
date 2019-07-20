package com.learning.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class PairRddExample {
	
	public static void main(String args[]) {
		SparkConf sparkConf = new SparkConf().setAppName("Pair Rdd Example")
				.setMaster("local[*]");
		SparkSession sparkSession = SparkSession.builder()
				.config(sparkConf)
				.getOrCreate();
		Dataset<Table1> table1Ds = table1Data(sparkSession);
		Dataset<Table2> table2Ds = table2Data(sparkSession);
		//groupByKeyExample(table1Ds);
		reduceByKeyExample(table1Ds);
	}
	
	public static JavaPairRDD<Integer,Integer> mapToPairExample(Dataset<Table1> empDs) {
		JavaPairRDD<Integer, Integer> pairRdd = empDs.javaRDD().mapToPair(new PairFunction<PairRddExample.Table1, Integer, Integer>() {

			@Override
			public Tuple2<Integer, Integer> call(Table1 emp) throws Exception {
				return new Tuple2<Integer, Integer>(emp.deptid, emp.id);//deptid,empid
			}
		});
		return pairRdd;
	}
	
	public static void groupByKeyExample(Dataset<Table1> empDs) {
		JavaPairRDD<Integer, Integer> pairRdd = mapToPairExample(empDs);
		System.out.println("before grouping");
		pairRdd.collect().forEach(keyvalue -> System.out.println(keyvalue._1 +" " +keyvalue._2));
		System.out.println("after grouping");
		pairRdd.groupByKey().collect().forEach(keyvalue -> System.out.println(keyvalue._1 +" " +keyvalue._2));
	}
	
	public static void reduceByKeyExample(Dataset<Table1> empDs) {
		JavaPairRDD<Integer, Integer> pairRdd = mapToPairExample(empDs);
		JavaPairRDD<Integer,Integer> reducedRdd = pairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2; //summing id + id doesn't make sense. This is just an example.
			}
		});
		reducedRdd.collect().iterator().forEachRemaining(keyvalue -> System.out.println(keyvalue._1 + " " +keyvalue._2));
	}
	
	/*public static void combineByKeyExample(Dataset<Table1> empDs) {
		JavaPairRDD<Integer,Integer> pairRdd = mapToPairExample(empDs);
		pairRdd.combin
	}*/
	
	public static Dataset<Table1> table1Data(SparkSession sparkSession) {
		Dataset<Table1> tableOneRows = sparkSession.read().textFile("F:\\eclipse-workspace\\testfiles\\emp.txt")
				.mapPartitions(new MapPartitionsFunction<String, Table1>() {
					public Iterator<Table1> call(Iterator<String> input) throws Exception {
						List<Table1> rows = new ArrayList<Table1>();
						while(input.hasNext()) {
							String row = input.next();
							String[] splitRow = row.split(",");
							Table1 table =  new Table1();
							table.id = Integer.parseInt(splitRow[0]);
							table.name = splitRow[1];
							table.deptid = Integer.parseInt(splitRow[2]);
							rows.add(table);
						}
						return rows.iterator();
					}
				}, Encoders.bean(Table1.class));
		return tableOneRows;
	}
	
	public static Dataset<Table2> table2Data(SparkSession sparkSession) {
		Dataset<Table2> tableTwoRows = sparkSession.read().textFile("F:\\eclipse-workspace\\testfiles\\dept.txt")
				.mapPartitions(new MapPartitionsFunction<String,Table2>() {

					@Override
					public Iterator<Table2> call(Iterator<String> input) throws Exception {
						List<Table2> rows = new ArrayList<Table2>();
						while(input.hasNext()) {
							String row = input.next();
							String[] splitRow = row.split(",");
							Table2 table2 = new Table2();
							table2.deptid = Integer.parseInt(splitRow[0]);
							table2.department = splitRow[1];
							rows.add(table2);
						}
						return rows.iterator();
					}
					
				}, Encoders.bean(Table2.class));
		return tableTwoRows;
	}

	public static class Table1 implements Serializable{
		private static final long serialVersionUID = 1L;
		private int id;
		private String name;
		private int deptid;
		
		public int getDeptid() {
			return deptid;
		}
		
		public void setDeptid(int deptid) {
			this.deptid = deptid;
		}
		
		public int getId() {
			return id;
		}
		
		public String getName() {
			return name;
		}

		public void setId(int id) {
			this.id = id;
		}
		
		public void setName(String name) {
			this.name = name;
		}
	}
	
	public static class Table2 {
		private int deptid;
		private String department;
		
		public String getDepartment() {
			return department;
		}
		
		public int getDeptid() {
			return deptid;
		}
		
		public void setDepartment(String department) {
			this.department = department;
		}
		
		public void setDeptid(int deptid) {
			this.deptid = deptid;
		}
	}
}
