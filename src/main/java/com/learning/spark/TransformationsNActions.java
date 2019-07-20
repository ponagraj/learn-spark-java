package com.learning.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;


public class TransformationsNActions {
	public static void main(String args[]) {
		System.setProperty("hadoop.home.dir","F:\\winutil");//change this path
		SparkConf sparkConf = new SparkConf()
				.setAppName("Transformations & Actions")
				.setMaster("local[*]");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		//orderByExample(sparkContext);
		//groupByExample();
		//unionExample();
		//unionAllExample();
		//intersectExample();
		//exceptExample();
		//denseRank_Rank_RowNumber_Example();
		lead_Lag_Example();
		//show_take_collect_example();
	}
	
	public static void orderByExample(JavaSparkContext jsc) {
		Dataset<Table1> table1Ds = table1Data();
		table1Ds.orderBy(functions.desc("id")).show();
	}
	
	public static void groupByExample() {
		Dataset<Table1> table1Ds = table1Data();
		table1Ds.filter("deptid != 10").groupBy("deptid").count().filter("count > 1").show();
	}
	
	public static void unionAllExample() {
		Dataset<Table1> table1Ds = table1Data();
		Dataset<Table2> table2Ds = table2Data();
		//deprecated
		table1Ds.select("deptid").unionAll(table2Ds.select("deptid")).show();
	}
	
	public static void intersectExample() {
		Dataset<Table1> table1Ds = table1Data();
		Dataset<Table2> table2Ds = table2Data();
		//by default only distinct values are picked in spark sql and my sql
		table1Ds.select("deptid").intersect(table2Ds.select("deptid")).show();
	}
	
	public static void unionExample() {
		//union works like union all.
		//union all is a shit. no more used.
		//check unionbyname as well
		Dataset<Table1> table1Ds = table1Data();
		Dataset<Table2> table2Ds = table2Data();
		table1Ds.select("deptid").union(table2Ds.select("deptid"))
		.distinct().orderBy(functions.desc("deptid")).show();
	}

	public static void exceptExample() {
		Dataset<Table1> table1Ds = table1Data();
		Dataset<Table2> table2Ds = table2Data();
		//print only distinct values in both spark sql and my sql
		table2Ds.select("deptid").except(table1Ds.select("deptid")).show();
	}
	
	public static void denseRank_Rank_RowNumber_Example() {
		Dataset<Table1> table1Ds = table1Data();
		table1Ds.withColumn("dense_rank", functions.dense_rank().over(Window.orderBy("id")))
			.withColumn("rank", functions.rank().over(Window.orderBy("id")))
		    .withColumn("row_number", functions.row_number().over(Window.orderBy("id")))
		    .show();
	}
	
	public static void lead_Lag_Example() {
		Dataset<Table1> table1Ds = table1Data();
		table1Ds.withColumn("leadValueOfId", functions.lead("id", 1).over(Window.orderBy("id")))
			.withColumn("lagValueofId", functions.lag("id", 1).over(Window.orderBy("id")))
			.show();
	}

	public static void show_take_collect_example() {
		Dataset<Table1> table1Ds = table1Data();
		table1Ds.show(2);
		table1Ds.takeAsList(3).forEach(entry -> System.out.println(entry.id));
		table1Ds.collectAsList().forEach(entry -> System.out.println(entry.id));; //full data. no selection
	}
	
	public static Dataset<Table1> table1Data() {
		SparkSession session = SparkSession.builder().appName("my java app").master("local[*]").getOrCreate();
		Dataset<Table1> tableOneRows = session.read().textFile("F:\\eclipse-workspace\\testfiles\\emp.txt")
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
	
	public static Dataset<Table2> table2Data() {
		SparkSession session = SparkSession.builder().appName("my java app").master("local[*]").getOrCreate();
		Dataset<Table2> tableTwoRows = session.read().textFile("F:\\eclipse-workspace\\testfiles\\dept.txt")
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
		/**
		 * 
		 */
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
