package com.eb.bi.rs.mras2.unifyrec.PrefRecResultMerge;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by LiuJie on 2016/04/10.
 */
public class PrefRecResultMergeReducer extends Reducer<Text,Text, Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	
        HashSet<String> set = new HashSet<String>();
        set.clear();
        StringBuffer sb = new StringBuffer(key.toString()+";");
        Iterator<Text> it = values.iterator();
        while (it.hasNext()) {
            String line = it.next().toString().trim().split(";", -1)[1];
            String [] fields=line.split("\\|");
            for(int i=0;i<fields.length;i++){
            	if(fields[i].split(",")[0]!=""&&!set.contains(fields[i].split(",")[0])){
            		set.add(fields[i].split(",")[0]);
            		sb.append(fields[i]);	
            		sb.append("|");
            	}
            }
        }
        context.write(new Text(sb.toString()), NullWritable.get());
    }
}
