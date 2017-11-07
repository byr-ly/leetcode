package com.eb.bi.rs.mras.authorrec.itemcf.filler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.RecommItemWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.recommend.PrefRecommItemGroup;
import com.eb.bi.rs.mras.authorrec.itemcf.recommend.RecommendItem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FillerReducer extends Reducer<Text, Text, Text, RecommItemWritable> {

    /**
     * @param values: 输入：预测用户偏好作者
     *                格式：key:msisdn value:第n偏好|authorid|bookid
     *                reduce out: 得分默认为-1
     *                key:msisdn|authorid|bookid|score|type
     */
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        List<PrefRecommItemGroup> prefItemsList = new ArrayList<PrefRecommItemGroup>();
        for (int i = 0; i < 4; i++) {
            PrefRecommItemGroup prefItems = new PrefRecommItemGroup();
            prefItemsList.add(prefItems);
        }
        for (Text value : values) {
            String[] strs = value.toString().split("\\|");
            int prefIndex = Integer.parseInt(strs[0]);
            PrefRecommItemGroup group = null;
            if (prefIndex == -1) {
                group = prefItemsList.get(3);
            } else {
                group = prefItemsList.get(prefIndex - 1);
            }
            String authorid = strs[1];
            String bookid = strs[2];
            RecommendItem item = new RecommendItem(authorid, bookid, -1);
            group.addItem(item);
        }
        for (int i = 0; i < prefItemsList.size(); i++) {
            PrefRecommItemGroup.checkDuplicate(i, prefItemsList, 10);
        }
        for (int i = 0; i < prefItemsList.size(); i++) {
            List<RecommendItem> items = prefItemsList.get(i).getRecommendItems();
            for (RecommendItem item : items) {
                //			String outStr = String.format("%s|%s|0|%d", item.getAuthorId(),
                //					item.getBookId(), i +51);
                int type = i + 51;
                RecommItemWritable writable = new RecommItemWritable(item, type);
                context.write(key, writable);
            }
        }
    }
}
