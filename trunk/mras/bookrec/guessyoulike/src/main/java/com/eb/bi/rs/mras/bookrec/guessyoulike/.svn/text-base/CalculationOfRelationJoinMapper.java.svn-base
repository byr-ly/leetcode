package com.eb.bi.rs.mras.bookrec.guessyoulike;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.TupleWritable;

import com.eb.bi.rs.mras.bookrec.guessyoulike.util.Similar;

public class CalculationOfRelationJoinMapper extends MapReduceBase 
implements Mapper<Text, TupleWritable, Text, Text>{

	@Override
	public void map(Text key, TupleWritable value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		//key:源图书

		//0:用户|图书打分|来源集
		String[] fields1 = value.get(0).toString().split("\\|");
		//1:目的图书|相似度向量
		String[] fields2 = value.get(1).toString().split("\\|");
		
		
		
		float userbookmark = Float.valueOf(fields1[1]);
		String[] sources = fields1[2].toString().split(",");
		
		Similar bookSimilar = new Similar();
		bookSimilar.init(fields2[1], ",");
		
		bookSimilar.similarMult(userbookmark);
		
		for(int i = 0; i != sources.length; i++){
			//key:用户|待推荐图书|源图书来源;val:源图书|计算后相似度向量
			output.collect(new Text(fields1[0]+"|"+fields2[0]+"|"+sources[i]),
					new Text(key.toString()+"|"+bookSimilar.tosimilarString(",")));
		}
		
		//output.collect(key, new Text(value.get(0).toString() +"|"+ value.get(1).toString()));
	}
}
