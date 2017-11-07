package com.eb.bi.rs.mras.authorrec.itemcf.order;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.ScoreWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.ScoreWritableComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 输入：key:msisdn|偏好n|偏好classid，value:authorid1|score1|authorid2|score2...
 * 输出：key:msisdn|偏好n|偏好classid，value按score从大到小取n个
 */
public class PrefAuthorFilterReducer
        extends Reducer<Text, ScoreWritable, Text, Text> {
    public void reduce(Text key, Iterable<ScoreWritable> values, Context context)
            throws IOException, InterruptedException {
        String[] strs = key.toString().split("\\|");
        String idxstr = strs[1];
        int prefIndex = Integer.parseInt(idxstr);
        int maxSize = 10;
        switch (prefIndex) {
            case 2:
                maxSize = 20;
                break;
            case 3:
                maxSize = 30;
                break;
            case -1:
                maxSize = 40;
            default:
                break;
        }
        List<ScoreWritable> scoreWritables = new ArrayList<ScoreWritable>();
        for (ScoreWritable value : values) {
            scoreWritables.add(value.clone());
        }
        ScoreWritableComparator comparator = new ScoreWritableComparator();
        Collections.sort(scoreWritables, comparator);
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < scoreWritables.size(); i++) {
            if (i < maxSize) {
                if (i > 0) {
                    buffer.append("|");
                }
                ScoreWritable obj = scoreWritables.get(i);
                buffer.append(obj.toString());
            }
        }
        context.write(key, new Text(buffer.toString()));
    }
}
