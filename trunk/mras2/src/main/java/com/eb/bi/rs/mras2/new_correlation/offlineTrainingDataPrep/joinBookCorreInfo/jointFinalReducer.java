package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.joinBookCorreInfo;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;

/**连接图书关联度，得到最后训练数据
 * 输入格式1：源图书A_推荐图书B A|图书A用户数|图书B用户数|图书AB共同用户数|前置信度|后置信|KULC|IR|classtype
 * 输入格式2：源图书_操作的图书 B|是否点击|用户|源图书|操作的图书|...|免费值|记录时间
 * 输出数据：是否点击|用户|源图书|操作的图书|...|免费值|前置信度|KULC|IR|记录时间
 * Created by linwanying on 2017/6/20.
 */
public class jointFinalReducer extends Reducer<Text, Text, Text, NullWritable> {
    private Text keyOut = new Text();
    private NullWritable valueOut = NullWritable.get();
    private Logger log = Logger.getLogger("jointFinalReducer");

    private HashSet<String> bookset = new HashSet<String>();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String bookinfo = "";
        for (Text value : values) {
            String[] fields = value.toString().split("\\|");
            if (fields[0].equals("A")) {
                bookinfo = value.toString().substring(2);
            } else {
                bookset.add(value.toString().substring(2));
            }
        }
        if (!bookinfo.equals("") && bookset.size() != 0) {
            for (String cur : bookset) {
                keyOut.set(cur.substring(0, cur.length()-8)+bookinfo+"|"+cur.substring(cur.length()-8));
                context.write(keyOut,valueOut);
            }
        }
        bookset.clear();
    }
}
