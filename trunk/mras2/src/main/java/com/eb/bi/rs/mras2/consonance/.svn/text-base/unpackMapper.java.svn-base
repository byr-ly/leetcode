package com.eb.bi.rs.mras2.consonance;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**对用户行为进行求和
 * Created by linwanying on 2016/11/16.
 */
public class unpackMapper extends Mapper<Object, Text, Text, NullWritable > {
   
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        String[] fields = value.toString().split("\\|");
        boolean flag = true;
        for(String i : fields){
        	if(flag){
        		flag = false;
        	}else{
        		context.write(new Text(fields[0]+"|"+i.split(",")[0]+"|"+i.split(",")[1]), NullWritable.get());
        	}
        }
    }
}
