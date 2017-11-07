package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.joinBookInfo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**输出格式：图书ID A|计费类型|书项总价格|是否连载|是否出版|总字数|版本分类|是否完本|待上架章节|已上架章节|推荐位级别|是否抢先|图书优先级|是否可下载|
 * Created by linwanying on 2017/6/19.
 */
public class getBookMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyOut = new Text();
    private Text valueOut = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
        if (fields.length < 17) return;
        keyOut.set(fields[0]);
        StringBuilder bookinfo = new StringBuilder();
        for (int i = 4; i <= 16; ++i) {
            bookinfo.append(fields[i]).append("|");
        }
        valueOut.set("A|"+bookinfo);
        context.write(keyOut, valueOut);
    }
}
