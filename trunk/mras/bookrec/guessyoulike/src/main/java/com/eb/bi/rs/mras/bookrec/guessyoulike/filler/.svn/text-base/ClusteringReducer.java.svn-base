package com.eb.bi.rs.mras.bookrec.guessyoulike.filler;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.eb.bi.rs.mras.bookrec.guessyoulike.util.TextPair;

public class ClusteringReducer extends Reducer<TextPair,Text, NullWritable, Text> {
	private MultipleOutputs<NullWritable,Text> mos;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		//初始化多输出
		mos = new MultipleOutputs<NullWritable,Text>(context);
	}
	
	@Override
	protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		String Id="";
		
		//按群体分离
		for(Text value:values){
			String[] fields = value.toString().split("\\|");
			
			if(fields[0].equals("0")){
				//0|用户|群id
				Id=fields[2];
				continue;
			}
				
			//待分群信息:1|内容
			if(Id.equals("")){
				mos.write(NullWritable.get(), new Text(value.toString().substring(2)), "noclustering/part");
				continue;
			}
				
			mos.write(NullWritable.get(), new Text(value.toString().substring(2)), "clustering-"+Id+"/part");
		}
	}
	
	@Override
	protected void cleanup(Context context
            ) throws IOException, InterruptedException {
		mos.close();
	}
}
