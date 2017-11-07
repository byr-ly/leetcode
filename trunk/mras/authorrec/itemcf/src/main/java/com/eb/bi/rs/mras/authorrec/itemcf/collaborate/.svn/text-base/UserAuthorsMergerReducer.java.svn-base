package com.eb.bi.rs.mras.authorrec.itemcf.collaborate;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.ScoreWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class UserAuthorsMergerReducer
        extends Reducer<Text, ScoreWritable, Text, Text> {
    /**
     * 同一用户打分的多个作者两两组合,对任意两个不同的作者，只组合一次
     * reduce out:
     */
    @Override
    public void reduce(Text key, Iterable<ScoreWritable> values, Context context)
            throws IOException, InterruptedException {
        List<ScoreWritable> scoreWritables = new ArrayList<ScoreWritable>();
        for (ScoreWritable value : values) {
            scoreWritables.add(value.clone());
        }
        if (scoreWritables.size() <= 1) {
            return;
        }
        for (ScoreWritable i : scoreWritables) {
            String idI = i.getAuthorId();
            double scoreI = i.getScore();
            for (ScoreWritable j : scoreWritables) {
                if (i == j) {
                    continue;
                }
                String idJ = j.getAuthorId();
                if (idI.compareTo(idJ) >= 0) {
                    continue;
                }
                String akey = String.format("%s|%s", idI, idJ);
                String aValue = String.format("%.3f|%.3f", scoreI, j.getScore());
                context.write(new Text(akey), new Text(aValue));
            }
        }
        scoreWritables.clear();
        scoreWritables = null;
    }

}
