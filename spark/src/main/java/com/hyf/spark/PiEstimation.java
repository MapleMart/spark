package com.hyf.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Pi Estimation Spark can also be used for compute-intensive tasks. This code estimates π by
 * "throwing darts" at a circle. We pick random points in the unit square ((0, 0) to (1,1)) and see
 * how many fall in the unit circle. The fraction should be π / 4, so we use this to get our
 * estimate.
 * 
 * 
 * @author 黄永丰
 * @createtime 2016年6月20日
 * @version 1.0
 */
public class PiEstimation
{
	public static int NUM_SAMPLES = 5;
	
	public static void main(String[] args)
	{
		
		List<Integer> l = new ArrayList<Integer>(NUM_SAMPLES);
		for (int i = 0; i < NUM_SAMPLES; i++)
		{
			l.add(i);
		}
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		long count = sc.parallelize(l).filter(new Function<Integer, Boolean>() {
			public Boolean call(Integer i)
			{
				double x = Math.random();
				double y = Math.random();
				return x * x + y * y < 1;
			}
		}).count();
		System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
	}

}
