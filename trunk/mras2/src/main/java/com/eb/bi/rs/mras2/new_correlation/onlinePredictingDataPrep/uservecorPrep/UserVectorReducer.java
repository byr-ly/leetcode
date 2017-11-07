package com.eb.bi.rs.mras2.new_correlation.onlinePredictingDataPrep.uservecorPrep;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * created by LiuJie on 2017/06/10.
 */
public class UserVectorReducer extends Reducer<Text, Text, Text, NullWritable> {
	String currentUserReadClasses =null;
	String currentUserBaseInfo = null;
	String currentUserPrefVector = null;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /**
         * 输入数据分两种。Key均为 msisdn。
         *
         * 1. 第一种值为A|用户阅读过的分类
         * 2. 第二种值为B|用户基础信息
         * 3. 第三种值为C|用户偏好向量
         * */
    	//当前用户
    	String userId = key.toString();
    	
        Iterator<Text> it = values.iterator();
        while (it.hasNext()) {

            String currentLine = it.next().toString();
            String[] fields = currentLine.split("\\|", -1);
            if (fields[0].equals("A")) {
                currentUserReadClasses = currentLine.substring(fields[1].length()+fields[0].length()+2, currentLine.length()).trim();
            }else if (fields[0].equals("B")) {
                currentUserBaseInfo = currentLine.substring(fields[1].length()+fields[0].length()+2, currentLine.length()).trim();
            }else if (fields[0].equals("C")) {
                if (fields.length < 19) {
                    System.out.printf("用户偏好向量 badline：" + currentLine);
                    return;
                }
                currentUserPrefVector = currentLine.substring(fields[1].length()+fields[0].length()+2, currentLine.length()).trim();
            }
        }
       
        if (currentUserPrefVector == null||currentUserBaseInfo==null) {
            return;
        }
        if(currentUserReadClasses==null) currentUserReadClasses="";
        
        /**
         * 拼接所有的用户特征
         */
        /*
         *  用户id;分群|分类属性权重|新书属性权重|名家属性权重|连载属性权重|付费方式属性权重|
		       促销属性权重|包月属性权重|男属性权重|女属性权重|俗属性权重|雅属性权重|热书属性权重|
		       第一分类偏好|第二分类偏好|第三分类偏好|用户偏执类型|前三分类相似分类(|多个);用户阅读过的分类（|多个）
		    ;近一周阅读章节数|下载量|购买章节|近一月阅读章节数|下载量|购买章节

         */
        
		String sb = userId +";"+ currentUserPrefVector.substring(0, currentUserPrefVector.length()-1) +";"+ currentUserReadClasses +";"+ currentUserBaseInfo;
		context.write(new Text(sb), NullWritable.get());


    }
}