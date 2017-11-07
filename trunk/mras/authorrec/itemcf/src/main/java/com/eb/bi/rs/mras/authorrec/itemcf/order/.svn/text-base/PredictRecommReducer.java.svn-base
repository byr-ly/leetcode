package com.eb.bi.rs.mras.authorrec.itemcf.order;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.RecommItemWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.recommend.PrefRecommItemGroup;
import com.eb.bi.rs.mras.authorrec.itemcf.recommend.RecommendItem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PredictRecommReducer
        extends Reducer<Text, RecommItemWritable, Text, RecommItemWritable> {
    /**
     * @param values: 输入：预测用户偏好作者（排序后）
     *                格式：key:msisdn, value:第n偏好|偏好classid|authorid1|bookid1|score1|author2|bookid2|score2|...
     *                规则：各偏好作家不重复，作者后面所跟图书分类与用户偏好相一致
     *                reduce out:
     *                key:msisdn|authorid|bookid|score|type
     */
    public void reduce(Text key, Iterable<RecommItemWritable> values, Context context)
            throws IOException, InterruptedException {
        List<PrefRecommItemGroup> prefItemsList = new ArrayList<PrefRecommItemGroup>();
        for (int i = 0; i < 4; i++) {
            PrefRecommItemGroup prefItems = new PrefRecommItemGroup();
            prefItemsList.add(prefItems);
        }
        for (RecommItemWritable value : values) {
            int prefIndex = value.getType();
            PrefRecommItemGroup group = null;
            if (prefIndex == -1) {
                group = prefItemsList.get(3);
            } else {
                group = prefItemsList.get(prefIndex - 1);
            }
            group.addItem(value.getItem().clone());
        }
        for (int i = 0; i < prefItemsList.size(); i++) {
            PrefRecommItemGroup.checkDuplicate(i, prefItemsList, 10);
        }
        for (int i = 0; i < prefItemsList.size(); i++) {
            List<RecommendItem> items = prefItemsList.get(i).getRecommendItems();
            for (RecommendItem item : items) {
                //		String outStr = String.format("%s|%s|%f|%d", item.getAuthorId(),
                //				item.getBookId(), item.getScore(), i +1);
                int type = i + 1;
                RecommItemWritable writable = new RecommItemWritable(item, type);
                context.write(key, writable);
            }
        }
    }
}
