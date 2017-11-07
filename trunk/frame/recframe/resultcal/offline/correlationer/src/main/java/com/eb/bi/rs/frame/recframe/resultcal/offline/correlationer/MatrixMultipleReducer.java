package com.eb.bi.rs.frame.recframe.resultcal.offline.correlationer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MatrixMultipleReducer extends Reducer<TextPair,Text, Text, Text> {
	private String whoisKey = "";
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		whoisKey = context.getConfiguration().get("Appconf.output.format.whoiskey");
	}
	
	@Override
	protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		//����map:
		//A|s//����ά�ȴ�����
		//Map<String, Float> A_score = new HashMap<String, Float>();
		//C|s//����ά��С�����
		Map<String, Float> C_score = new HashMap<String, Float>();
		
		String newkey = "";
		float newval = 0;
		
		String keyA = "";
		float valA = 0;
		String keyC = "";
		float valC = 0;
		
		for(Text value:values){
			//flag|A/C|s
			String[] fields = value.toString().split("\\|");
			
			if(fields[0].equals("0")){//����ά�ȴ�����
				//add:A|s
				//A_score.put(fields[1], Float.valueOf(fields[2]));
				keyA = fields[1];
				valA = Float.valueOf(fields[2]);
				
				Iterator<Entry<String, Float>> it = C_score.entrySet().iterator();
				while (it.hasNext()){
					Map.Entry<String, Float> entryC = (Map.Entry<String, Float>) it.next();
					keyC = entryC.getKey();
					valC = entryC.getValue();
					
					if(whoisKey.equals("table.1")){
						newkey = keyA + "|" + keyC;
					}
					else{
						newkey = keyC + "|" + keyA;
					}
					newval = valA * valC;
					
					context.write(new Text(newkey),new Text(""+newval));
				}
				
			}
			else{//����ά��С�����
				//add:C|s
				C_score.put(fields[1], Float.valueOf(fields[2]));
			}
		}
		
		/*
		Iterator<Entry<String, Float>> iter = A_score.entrySet().iterator();
		while (iter.hasNext()){
			Map.Entry<String, Float> entryA = (Map.Entry<String, Float>) iter.next();
			keyA = entryA.getKey();
			valA = entryA.getValue();
			
			Iterator<Entry<String, Float>> it = C_score.entrySet().iterator();
			while (it.hasNext()){
				Map.Entry<String, Float> entryC = (Map.Entry<String, Float>) it.next();
				keyC = entryC.getKey();
				valC = entryC.getValue();
				
				newkey = keyA + "|" + keyC;
				newval = valA * valC;
				
				context.write(new Text(newkey),new Text(""+newval));
			}	
		}
		*/
	}
}
