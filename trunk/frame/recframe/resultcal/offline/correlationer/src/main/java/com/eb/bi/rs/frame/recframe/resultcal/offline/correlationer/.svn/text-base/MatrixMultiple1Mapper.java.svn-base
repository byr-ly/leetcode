package com.eb.bi.rs.frame.recframe.resultcal.offline.correlationer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMultiple1Mapper extends Mapper<LongWritable, Text, TextPair, Text>{
	private String dataformatType;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		//���ز��׿��ļ�
		dataformatType = context.getConfiguration().get("Appconf.data.input.format.type1");
	}
	
	//����ά�ȴ�����
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		//A|B(key)|s
		String[] fields = value.toString().split("\\|");
		
		if(dataformatType.equals("s")){
			//��?��(�з��޷־��)
			if(fields.length == 3){
				context.write(new TextPair(fields[1],"1"),new Text("0|"+fields[0]+"|"+fields[2]));
			}	
			else if(fields.length == 2){
				context.write(new TextPair(fields[1],"1"),new Text("0|"+fields[0]+"|1"));
			}
			else{
				System.out.println("bad record : "+value.toString());
				return;
			}
		}
		else{
			//��?��(�޷����)
			for(int i = 1;i != fields.length;i++){
				context.write(new TextPair(fields[i],"1"),new Text("0|"+fields[0]+"|1"));
			}
		}
	}
}
