package com.eb.bi.rs.frame2.evaluation.online.userRecAvgOLmtx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by houmaozheng on 2017/1/4.
 */
public class userRecNurMapper extends Mapper<Object, Text, Text, Text> {
    private String fieldDelimiter;
    private String fieldDigit;// 位数
	private String fieldNum;
	private Map<String, String> fieldNumber = new HashMap<String, String>();
	
    protected void setup(Context context) {

        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
		fieldDigit = conf.get("field.digit");
		fieldNum = conf.get("field.num");
		for (int i = 1; i <= Integer.parseInt(fieldNum); i++) {
			String confSet = "field.number." + i;
			fieldNumber.put(conf.get(confSet), Integer.toString(i));
		}
    }

    /*
    输入数据:
        用户推荐位图书数（去重）表
        msisdn用户 | 推荐位 | 用户推荐位展现图书数（去重） | 时间

    输出数据:
        key: msisdn用户 | 推荐位 | 标识
        value: "Nur" | 用户推荐位展现图书数（去重）
    */
    public void map(Object text, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(fieldDelimiter);
        int digit = Integer.parseInt(fieldDigit);
    	int abs = Math.abs(digit);
    	int numLength = fields[0].length();
    	boolean flag = true;
    	
        if (fields.length == 4) {
        	if (fields[0] != null && fields[1] != null &&
                    fields[2] != null && digit!=0) {// 数据不为空
            	if(digit>0){
            		for(Map.Entry<String, String> entry : fieldNumber.entrySet() ){
                		String[] number = entry.getKey().split(",");
                		for(int i=0;i<number.length;i++){
                			if(fields[0].substring(digit-1,digit).equals(number[i])){
                				context.write(new Text(fields[0] + "|" + fields[1] + "|" + entry.getValue()), new Text("Nur|" + fields[2]));
                				flag = false;
                			}
                	}
                }
            }else{
            	for(Map.Entry<String, String> entry : fieldNumber.entrySet() ){
            		String[] number = entry.getKey().split(",");
            		for(int i=0;i<number.length;i++){
            			if(fields[0].substring(numLength-abs,numLength-abs+1).equals(number[i])){
            				context.write(new Text(fields[0] + "|" + fields[1] + "|" + entry.getValue()), new Text("Nur|" + fields[2]));
            				flag = false;
            			}
            		}
            	}
            }
            	if(flag==true){
            		context.write(new Text(fields[0] + "|" + fields[1] + "|" + "0"), new Text("Nur|" + fields[2]));
            	}
          }
//        	if(fields[0] != null && fields[1] != null &&
//                    fields[2] != null && digit==0){
//        		context.write(new Text(fields[0] + "|" + fields[1] + "|" + "0"), new Text("Nur|" + fields[2]));
//        	}
        }
    }
}
