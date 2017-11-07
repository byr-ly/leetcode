package com.eb.bi.rs.frame2.recframe.resultcal.offline.selector.mr;

import com.eb.bi.rs.frame2.recframe.resultcal.offline.selector.util.StringDoublePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Iterator;
import java.util.TreeSet;

public class OrderedTopNSelectorCombiner extends Reducer<Text, StringDoublePair, Text, StringDoublePair> {
    private int selectorNum;

    @Override
    protected void reduce(Text key, Iterable<StringDoublePair> values, Context context) throws java.io.IOException, InterruptedException {
        TreeSet<StringDoublePair> set = new TreeSet<StringDoublePair>();
        for (StringDoublePair pair : values) {
            if (set.size() < selectorNum) {
                set.add(new StringDoublePair(pair));
            } else {
                if (pair.compareTo(set.first()) > 0) {
                    set.remove(set.first());
                    set.add(new StringDoublePair(pair));
                }
            }
        }
        Iterator<StringDoublePair> iter = set.iterator();
        while (iter.hasNext()) {
            context.write(key, iter.next());
        }
    }

    @Override
    protected void setup(Context context) throws java.io.IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        selectorNum = conf.getInt("select.number", 1);
    }
}

