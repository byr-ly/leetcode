package com.eb.bi.rs.mras.unifyrec.attributeengin.UserReadHistory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

/**Created by LiuJie on 2016/04/10.
 */
public class UserReadHistoryReducer extends Reducer<Text,Text, Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<Integer> set = new HashSet<Integer>();
        Iterator<Text> it = values.iterator();
        while (it.hasNext()) {
            String bookid = it.next().toString();
            try {
                set.add(Integer.parseInt(bookid));
            } catch (NumberFormatException e) {
                System.out.println("bad line : " + bookid);
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
