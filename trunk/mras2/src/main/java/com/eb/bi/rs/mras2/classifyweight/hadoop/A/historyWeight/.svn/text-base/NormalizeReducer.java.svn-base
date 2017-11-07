package com.eb.bi.rs.mras2.classifyweight.hadoop.A.historyWeight;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**归一化
 * Created by linwanying on 2017/4/12.
 */
public class NormalizeReducer  extends Reducer<Text, Text, Text, NullWritable> {
    /**
     * 输入数据分两种。Key均为 msisdn。
     *
     * 1. 第一种值为A|用户总得分
     * 2. 第二种值为B|用户分类得分
     * 输出数据：
     *       msisdn用户_分类|得分
     * */
    private static Logger log = Logger.getLogger("linwanying");
    private Text keyout = new Text();
    private NullWritable valueout = NullWritable.get();
    private HashMap<String, Double> classscore = new HashMap<String, Double>();
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        classscore.clear();
        Iterator<Text> it = values.iterator();
        double sum = 0;
        while (it.hasNext()) {
            String currentLine = it.next().toString().trim();
            String[] fields = currentLine.split("\\|", -1);
            if (fields[0].equals("A")) {
                sum = Double.parseDouble(fields[1]);
            } else if (fields[0].equals("B")) {
                classscore.put(fields[1], Double.valueOf(fields[2]));
            }
        }
//        log.info("msisdn: " + key + ", " + "mapsize: " + classscore.size());
        if (sum == 0) {
            return;
        }
        double result = 0;
//        DecimalFormat df = new DecimalFormat("#0.0000");

        for (String classid : classscore.keySet()) {
            StringBuilder keyOut = new StringBuilder(key.toString());
            result = classscore.get(classid) / sum;
            keyOut.append("_").append(classid).append("|").append(String.valueOf(result));
            keyout.set(keyOut.toString());
            context.write(keyout, valueout);
        }
    }
}