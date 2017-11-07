package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.recClick;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

/**根据点击结果和推荐列表获得用户点击完整数据
 * 输入格式： A|源图书|推荐图书|时间，B|推荐类型|推荐图书|时间
 * 输出格式： 是否点击|用户|源图书|操作的图书|操作类型（阅读、订购）|记录时间
 * Created by linwanying on 2017/6/15.
 */
public class getRecClickReducer extends Reducer<Text, Text, Text, NullWritable> {
    private Text keyOut = new Text();
    private NullWritable valueOut = NullWritable.get();
    private HashMap<String, HashSet<String>> reclists = new HashMap<String, HashSet<String>>();
    private HashMap<String, HashSet<String>> click = new HashMap<String, HashSet<String>>();
    private Logger log = Logger.getLogger("getRecClickReducer");
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] tmpline = value.toString().split("\\|");
            if (tmpline[0].equals("A")) {
                log.info(value.toString());
                HashSet<String> bookset = new HashSet<String>();
                bookset.addAll(Arrays.asList(tmpline).subList(2, tmpline.length - 1));
                reclists.put(tmpline[tmpline.length - 1]+"_"+tmpline[1], bookset);
            }
            if (tmpline[0].equals("B")) {
                log.info(value.toString());
                HashSet<String> books = new HashSet<String>();
                if (click.containsKey(tmpline[3])) {
                    books = click.get(tmpline[3]);
                }
                books.add(tmpline[2]);
                click.put(tmpline[3], books);
            }
        }
        HashSet<String> result = new HashSet<String>();
        for (String k : reclists.keySet()) {
            String time = k.split("_")[0];
            String oriBook = k.split("_")[1];
            if (!click.containsKey(time)) continue;
            result.clear();
            result.addAll(click.get(time));
            result.retainAll(reclists.get(k));
            if (result.size() == 0) continue;
            for (String rec : reclists.get(k)) {
                if (click.get(time).contains(rec)) {
                    keyOut.set("1|"+key.toString()+"|"+oriBook+"|"+rec+"|3|"+time);
                } else {
                    keyOut.set("0|"+key.toString()+"|"+oriBook+"|"+rec+"|3|"+time);
                }
                log.info(keyOut+" "+valueOut);
                context.write(keyOut, valueOut);
            }
        }
        click.clear();
        reclists.clear();
    }
}
