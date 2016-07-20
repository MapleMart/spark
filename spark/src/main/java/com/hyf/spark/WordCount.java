package com.hyf.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 学习官网上的案例 字数 在这个例子中，我们使用了一些转变，建设（字符串，整数）对所谓的数据集数，然后将其保存到一个文件中。
 * 导入包：在spark1.6.0包的lib下spark-assembly-1.6.0-hadoop2.6.0.jar
 * 在hadoop-2.6.0\share\hadoop\common\hadoop-common-2.6.0.jar
 * 在hadoop-2.6.0\share\hadoop\common\lib\下的包 log4j-1.2.17.jar slf4j-api-1.7.10.jar
 * slf4j-log4j12-1.7.10.jar commons-configuration-1.6.jar guava-11.0.2.jar 要启动hadoop
 * @author 黄永丰
 * @createtime 2016年6月17日
 * @version 1.0
 */
public class WordCount
{

	public static void main(String[] args)
	{
		String logFile = "hdfs://192.168.1.20:9000/test1/b.txt"; // 可以是你系统上任意的英文 txt
																	// 文件或是hadoop上的hdfs的文件
		// Exception in thread "main" org.apache.spark.SparkException: A master URL must be set in
		// your configu
		// 就要.setMaster("local[*]")
		/**
		 * 传递给spark的master url可以有如下几种：
		 * 
		 * local 本地单线程 
		 * local[K] 本地多线程（指定K个内核） 
		 * local[*] 本地多线程（指定所有可用内核） 
		 * spark://HOST:PORT 连接到指定的 Spark standalone cluster master，需要指定端口。 
		 * mesos://HOST:PORT 连接到指定的 Mesos 集群，需要指定端口。
		 * yarn-client客户端模式 连接到 YARN 集群。需要配置 HADOOP_CONF_DIR。 
		 * yarn-cluster集群模式 连接到 YARN 集群 。需要配置 HADOOP_CONF_DIR。
		 * 
		 * 作者：Markus Hao 链接：http://www.zhihu.com/question/23967309/answer/26243256 来源：知乎
		 * 著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。
		 */
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> textFile = sc.textFile(logFile);
		JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s)
			{
				return Arrays.asList(s.split(" "));
			}
		});
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s)
			{
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b)
			{
				return a + b;
			}
		});
		counts.saveAsTextFile("hdfs://192.168.1.20:9000/test1/b3.txt");// hadoop上的新建的hdfs的文件
	}

}
