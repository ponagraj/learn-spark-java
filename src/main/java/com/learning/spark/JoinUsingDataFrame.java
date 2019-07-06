package com.learning.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JoinUsingDataFrame {
	public static void main(String args[]) {
		System.setProperty("hadoop.home.dir","F:\\winutil");
		SparkConf conf = new SparkConf()
				.setAppName("my appp")
				.setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		//JonTables.joinTables(jsc);
		JoinUsingDataFrame.broadcastExample(jsc);
	}
	
	private static void broadcastExample(JavaSparkContext jsc) {
		SparkSession session = SparkSession.builder().appName("my java app").master("local[*]").getOrCreate();
		Dataset<Row> tableOneRows = session.read().schema(getTableOneSchema()).csv("src\\main\\res\\emp.txt");
		Dataset<Row> tableTwoRows = session.read().csv("src\\main\\res\\dept.txt");
		tableOneRows.show();
		tableTwoRows.select("_c1").show();
	}
	
	private static StructType getTableOneSchema() {
		StructType schema = new StructType(new StructField[] {
			new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
			new StructField("name",DataTypes.StringType, true, Metadata.empty())
		});
		return schema;
	}
}
