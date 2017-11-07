package com.eb.bi.rs.mras.unifyrec.attributeengin.UserClassPref;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by LiuJie on 2016/04/10.
 */
public class UserClassPrefReducer extends Reducer<Text, Text, Text,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {   	
        /**
         * 输入数据分两种。Key均为 msisdn。
         *
         * 1. 第一种值为A|用户阅读过的分类
         * 2. 第二种值为B|用户偏好向量
         * 输出数据：
         *       msisdn用户 |用户群|偏执程度|前三分类 |相似分类 |历史阅读分类 
         * */     
    	
        StringBuffer currentUserReadClasses =null;
        StringBuffer UserReadClasses =null;
        StringBuffer prefClass_simClass = null;
        HashSet<String> topsimclasses = null;
        
        Iterator<Text> it = values.iterator();
        
        while (it.hasNext()) {	
        	
            String currentLine = it.next().toString().trim();        
            String[] fields = currentLine.split("\\|", -1);
            if (fields[0].equals("A")) {
            	currentUserReadClasses = new StringBuffer();
            	for (int i = 2; i < fields.length; i++) { 
            		if(i==fields.length-1){
            			currentUserReadClasses.append(fields[i]); 
            			break;
            		}
            		currentUserReadClasses.append(fields[i]); 
            		currentUserReadClasses.append(",");            		
                }
            }else if (fields[0].equals("C")) {
                if (fields.length < 19) {
                    return;
                }      
                prefClass_simClass = new StringBuffer();
                //msisdn用户 |用户群|偏执程度|
                prefClass_simClass.append(fields[1]+"|"+fields[2]+"|"+fields[18]+"|");
                //前三分类 |相似分类 
                topsimclasses = new HashSet<String>();
                for (int i = 15; i < fields.length; i++) {//15 16 17 18...
                	if(i==17){
                    	prefClass_simClass.append(fields[i]+"|"); 
                    	topsimclasses.add(fields[i]);
            			continue;
            		}
                	if(i==18){
                		continue;
                	}
                    if(i==(fields.length-2)){
                    	prefClass_simClass.append(fields[i]);
                    	topsimclasses.add(fields[i]);
                    	break;
                    }                
                    prefClass_simClass.append(fields[i]); 
                    topsimclasses.add(fields[i]);
                    prefClass_simClass.append(",");
                } 
            }
        }
        if (prefClass_simClass == null) {
            return;
        }
        prefClass_simClass.append("|");
        if(currentUserReadClasses!=null){
        	String [] fields = currentUserReadClasses.toString().split(",");
            UserReadClasses = new StringBuffer();
            for (int i=0;i<fields.length;i++){
            	if(!topsimclasses.contains(fields[i])){
            		UserReadClasses.append(fields[i]);
            		UserReadClasses.append(",");
            	}
            }       
            if(UserReadClasses!=null&&UserReadClasses.length()!=0){
            	//|历史阅读分类 
            	UserReadClasses.deleteCharAt(UserReadClasses.length()-1);
            	prefClass_simClass.append(UserReadClasses.toString().trim());
            }
            else{
                prefClass_simClass.append(" |");
            }
        }
        context.write(new Text(prefClass_simClass.toString()),NullWritable.get());             
   }
}