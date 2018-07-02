package com.hadoop.myhadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final int MISSING = 9999;

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String year = line.substring(15, 19);
		int airTemperatture;
		if (line.charAt(87) == '+') {
			airTemperatture = Integer.parseInt(line.substring(88, 92));
		} else {
			airTemperatture = Integer.parseInt(line.substring(87, 92));
		}

		String quality = line.substring(92, 93);
		if (airTemperatture != MISSING && quality.matches("[01459]")) {
			context.write(new Text(year), new IntWritable(airTemperatture));
		}
	}
}
