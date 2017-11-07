package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.joinBookInfo;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;

/**输入1：操作的图书 B|是否点击|用户|源图书|操作的图书|操作类型（阅读、订购）|记录时间
 * 输入2：图书ID A|计费类型|书项总价格|是否连载|是否出版|总字数|版本分类|是否完本|待上架章节|已上架章节|推荐位级别|是否抢先|图书优先级|是否可下载|
 * 输出： 是否点击|用户|源图书|操作的图书|...|计费类型|书项总价格|是否连载|...|记录时间
 * Created by linwanying on 2017/6/19.
 */
public class getUserBookReducer extends Reducer<Text, Text, Text, NullWritable> {
    private Text keyOut = new Text();
    private NullWritable valueOut = NullWritable.get();
    private Logger log = Logger.getLogger("getUserBookReducer");

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
            log.info(value.toString());
        }
        if (!bookinfo.equals("") && bookset.size() != 0) {
            for (String cur : bookset) {
                keyOut.set(cur.substring(0, cur.length()-8)+bookinfo+cur.substring(cur.length()-8));
                context.write(keyOut,valueOut);
            }
        }
        bookset.clear();
    }
}