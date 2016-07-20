package com.hyf.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * In Spark, a DataFrame is a distributed collection of data organized into named columns. Users can
 * use DataFrame API to perform various relational operations on both external data sources and
 * Spark’s built-in distributed collections without providing specific procedures for processing
 * data. Also, programs based on DataFrame API will be automatically optimized by Spark’s built-in
 * optimizer, Catalyst.
 * 
 * Text Search In this example, we search through the error messages in a log file.
 * @author 黄永丰
 * @createtime 2016年6月20日
 * @version 1.0
 */
public class DataFrameAPIExamples
{

	public static void main(String[] args)
	{
		// Creates a DataFrame having a single column named "line"
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> textFile = sc.textFile("hdfs://192.168.1.20:9000/test1/b.txt");
		JavaRDD<Row> rowRDD = textFile.map(new Function<String, Row>() {
			public Row call(String line) throws Exception
			{
				return RowFactory.create(line);
			}
		});
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("line", DataTypes.StringType, true));
		StructType schema = DataTypes.createStructType(fields);

		// Creates a DataFrame based on a table named "people"
		// stored in a MySQL database.
		SQLContext sqlContext = new SQLContext(sc);
		String url = "jdbc:mysql://192.168.87.89:3306/test?user=root;password=root";
		DataFrame df = sqlContext.read().format("jdbc").option("url", url).option("dbtable", "people").load();

		// Looks the schema of this DataFrame.
		df.printSchema();

		// Counts people by age
		DataFrame countsByAge = df.groupBy("age").count();
		countsByAge.show();

		// Saves countsByAge to S3 in the JSON format.
		countsByAge.write().format("json").save("hdfs://192.168.1.20:9000/test1/b2.txt");

	}

}
