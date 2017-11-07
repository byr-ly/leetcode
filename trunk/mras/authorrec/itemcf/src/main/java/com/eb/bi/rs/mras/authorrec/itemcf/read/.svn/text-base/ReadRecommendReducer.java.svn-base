package com.eb.bi.rs.mras.authorrec.itemcf.read;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.RecommItemWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.recommend.RecommendItem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReadRecommendReducer
        extends Reducer<Text, Text, Text, RecommItemWritable> {
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        List<RecommendItem> orderItems = new ArrayList<RecommendItem>();
        for (Text value : values) {
            String[] strs = value.toString().split("\\|");
            String authorid = strs[0];
            String bookid = strs[1];
            double score = Double.parseDouble(strs[2]);
            RecommendItem item = new RecommendItem(authorid, bookid, score);
            addRecommItem(item, orderItems);
        }
        for (int i = 0; i < orderItems.size(); i++) {
            RecommendItem item = orderItems.get(i);
            //		String outStr = String.format("%s|%s|%f|0", item.getAuthorId(),
            //				item.getBookId(), item.getScore());
            RecommItemWritable writable = new RecommItemWritable(item, 0);
            context.write(key, writable);
        }
    }

    private int addRecommItem(RecommendItem item, List<RecommendItem> orderItems) {
        int pos = -1;
        boolean bAdd = false;
        for (int i = 0; i < orderItems.size(); i++) {
            if (orderItems.get(i).getScore() < item.getScore()) {
                orderItems.add(i, item);
                bAdd = true;
                pos = i;
                break;
            }
        }
        if (!bAdd) {
            if (orderItems.size() < 10) {
                pos = orderItems.size();
                orderItems.add(item);
            }

        }
        while (orderItems.size() > 10) {
            orderItems.remove(10);
        }
        return pos;
    }
}