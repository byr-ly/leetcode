package com.eb.bi.rs.mras.bookrec.personalrec;

import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PersonalResultMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object o, Text value, Context context) throws IOException, InterruptedException {
	
		String[] fields = value.toString().split("\\|", 2);
		if (fields.length != 2) {
			return;
		}

		context.write(new Text(fields[0]), new Text(fields[1]));
	}

}
