package com.learning.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JonTables {
	public static void main(String args[]) {
		System.setProperty("hadoop.home.dir","F:\\winutil");
		SparkConf conf = new SparkConf()
				.setAppName("my appp")
				.setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JonTables joinTables = new JonTables();
		JonTables.joinTables(jsc);
	}
	
	private static void joinTables(JavaSparkContext jsc) {
		SparkSession session = SparkSession.builder().appName("my java app").master("local[*]").getOrCreate();
		JavaRDD<String> rdd = jsc.textFile("F:\\eclipse-workspace\\testfiles\\emp.txt");
		Dataset<Table1> tableOneRows = session.read().textFile("F:\\eclipse-workspace\\testfiles\\emp.txt")
				.mapPartitions(new MapPartitionsFunction<String, Table1>() {
					public Iterator<Table1> call(Iterator<String> input) throws Exception {
						List<Table1> rows = new ArrayList<Table1>();
						while(input.hasNext()) {
							String row = input.next();
							String[] splitRow = row.split(",");
							Table1 table =  new Table1();
							table.empId = Integer.parseInt(splitRow[0]);
							table.empName = splitRow[1];
							table.deptId = Integer.parseInt(splitRow[2]);
							rows.add(table);
						}
						return rows.iterator();
					}
				}, Encoders.bean(Table1.class));
		//System.out.println("rows:: "+tableOneRows.count());
		//tableOneRows.show(false);
		
		Dataset<Table2> tableTwoRows = session.read().textFile("F:\\eclipse-workspace\\testfiles\\dept.txt")
				.mapPartitions(new MapPartitionsFunction<String,Table2>() {

					public Iterator<Table2> call(Iterator<String> input) throws Exception {
						List<Table2> rows = new ArrayList<Table2>();
						while(input.hasNext()) {
							String row = input.next();
							String[] splitRow = row.split(",");
							Table2 table2 = new Table2();
							table2.deptId = Integer.parseInt(splitRow[0]);
							table2.deptName = (splitRow[1]);
							rows.add(table2);
						}
						return rows.iterator();
					}
					
				}, Encoders.bean(Table2.class));
		//tableTwoRows.show();
		
	//do cross join
		Dataset<Row> crossJoin = tableOneRows.crossJoin(tableTwoRows);
		crossJoin.show();
		System.out.println("cross count: " +crossJoin.count());
		
		//do inner join -- 
		//1) all the columns from both the tables will be displayed
		//2) only the matching rows will be displayed
		Dataset<Row> innerJoin = tableOneRows.join(tableTwoRows, tableOneRows.col("deptId").equalTo(tableTwoRows.col("deptId")));
		innerJoin.show();
		
		//do simple outer join
		//1) all the columns from both the tables will be displayed
		//2) even unmatching rows will be displayed will null. Looks like FULL OUTER JOIN?
		// OUTER, FULL, FULLOUTER all same
		Dataset<Row> outerJoin = tableOneRows.join(tableTwoRows, tableOneRows.col("deptId").equalTo(tableTwoRows.col("deptId")),"outer");
		outerJoin.show();

		//do full outer join
		//1) all the columns from both the tables will be displayed
		//2) even unmatching rows will be displayed will null.
		Dataset<Row> fullOuterJoin = tableOneRows.join(tableTwoRows, tableOneRows.col("deptId").equalTo(tableTwoRows.col("deptId")),"fullouter");
		fullOuterJoin.show();
		
		//do left outer join
		//1) all the columns from both the tables will be included
		//2) left side coloumns are also included and right side columns would be null
		// LEFT, LEFTOUT same
		//RIGHT, RIGHTOUT same
		Dataset<Row> leftOuterJoin = tableOneRows.join(tableTwoRows, tableOneRows.col("deptId").equalTo(tableTwoRows.col("deptId")),"leftouter");
		leftOuterJoin.show();
		
		
		//do left semi join
		//1) only the columns from left table will be part of the output.
		//2) semi join returns rows which ARE MATCHING
		Dataset<Row> leftSemiJoin = tableOneRows.join(tableTwoRows, tableOneRows.col("deptId").equalTo(tableTwoRows.col("deptId")),"leftsemi");
		leftSemiJoin.show();
		leftSemiJoin.count();
		
		//do left ant join
		//1) only the columns from left table will be part of the output.
		//2) semi join returns rows which ARE NOT MATCHING
		Dataset<Row> leftAntiJoin = tableOneRows.join(tableTwoRows, tableOneRows.col("deptId").equalTo(tableTwoRows.col("deptId")),"leftanti");
		leftAntiJoin.show();
		leftAntiJoin.count();

		//selecting which columns we want
		Dataset<Row> selectCols = tableOneRows
				.join(tableTwoRows, tableOneRows.col("deptId").equalTo(tableTwoRows.col("deptId")),"inner")
				.select(tableOneRows.col("empId"), tableOneRows.col("empName"), tableTwoRows.col("deptName"));
		selectCols.show();
		selectCols.count();
		
		
		System.out.println("debug point");
	}
	
	public static class Table2 {
		private int deptId;
		private String deptName;
		
		public int getDeptId() {
			return deptId;
		}
		
		public void setDeptId(int deptId) {
			this.deptId = deptId;
		}
		
		public String getDeptName() {
			return deptName;
		}
		
		public void setDeptName(String deptName) {
			this.deptName = deptName;
		}
		
	}
	
	public static class Table1 implements Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private int empId;
		private String empName;
		private int deptId;

		public int getDeptId() {
			return deptId;
		}
		
		public void setDeptId(int deptId) {
			this.deptId = deptId;
		}
		
		public int getEmpId() {
			return empId;
		}
		
		public void setEmpId(int empId) {
			this.empId = empId;
		}
		
		public String getEmpName() {
			return empName;
		}
		
		public void setEmpName(String empName) {
			this.empName = empName;
		}
	}
	private void usingDataFrame() {
		
	}
}
