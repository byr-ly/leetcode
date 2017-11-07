package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.getAndJoinUBinfo;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;

/**连接用户和图书交叉信息
 * 输入格式1：用户_图书ID A|分类值|新书值|性别值|雅俗值|连载值|免费值
 * 输入格式2：用户_操作图书 B|是否点击|用户|源图书|操作的图书|...|计费类型|书项总价格|是否连载|...|记录时间
 * 输出格式： 是否点击|用户|源图书|操作的图书|...|计费类型|书项总价格|是否连载|...|分类值|新书值|性别值|雅俗值|连载值|免费值|记录时间
 * Created by linwanying on 2017/6/20.
 */
public class jointUBinfoReducer extends Reducer<Text, Text, Text, NullWritable> {
    private Text keyOut = new Text();
    private NullWritable valueOut = NullWritable.get();
    private Logger log = Logger.getLogger("jointUBinfoReducer");

    private HashSet<String> userbookset = new HashSet<String>();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String userbookinfo = "";
        for (Text value : values) {
            String[] fields = value.toString().split("\\|");
            if (fields[0].equals("A")) {
                userbookinfo = value.toString().substring(2);
            }
            if (fields[0].equals("B")) {
                userbookset.add(value.toString().substring(2));
            }
        }
        if (!userbookinfo.equals("") && userbookset.size() != 0) {
            for (String cur : userbookset) {
                keyOut.set(cur.substring(0, cur.length()-8)+userbookinfo+"|"+cur.substring(cur.length()-8));
                context.write(keyOut,valueOut);
            }
        }
        userbookset.clear();
    }
}