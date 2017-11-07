package com.eb.bi.rs.mras.unifyrec.attributeengin.UserReadClasses;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by LiMingji on 2015/11/10.
 */
public class UserReadClassesReducer extends Reducer<Text,Text, Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<Integer> set = new HashSet<Integer>();
        Iterator<Text> it = values.iterator();
        while (it.hasNext()) {
            String classes = it.next().toString();
            try {
                set.add(Integer.parseInt(classes));
            } catch (NumberFormatException e) {
                System.out.println("bad line : " + classes);
            }
        }
        if (set.size() == 0) {
            return ;
        }
        StringBuffer sb = new StringBuffer(key.toString());
        for (Integer integer : set) {
            sb.append("|");
            sb.append(integer);
        }
        context.write(new Text(sb.toString()), new Text());
    }
}
