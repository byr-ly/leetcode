package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.getAndJoinUserInfo;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;

/**用户 A|chrg_dmd_fee点播费用|dnld_cnt下载量|vst_pv访问量(近一周)|chrg_dmd_fee点播费用|dnld_cnt下载量|vst_pv访问量(近一月)
 * 用户 B|是否点击|用户|源图书|操作的图书|...|免费值|前置信度|KULC|IR|记录时间
 * Created by linwanying on 2017/6/23.
 */
public class jointUserStatisReducer extends Reducer<Text, Text, Text, NullWritable> {
    private Text keyOut = new Text();
    private NullWritable valueOut = NullWritable.get();
    private Logger log = Logger.getLogger("jointUserStatisReducer");

    private HashSet<String> userset = new HashSet<String>();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String userinfo = "";
        for (Text value : values) {
            String[] fields = value.toString().split("\\|");
            if (fields[0].equals("A")) {
                userinfo = value.toString().substring(2);
            }
            if (fields[0].equals("B")) {
                userset.add(value.toString().substring(2));
            }
            log.info(value.toString());
        }
        if (!userinfo.equals("") && userset.size() != 0) {
            for (String cur : userset) {
                keyOut.set(cur.substring(0, cur.length()-8)+userinfo+"|"+cur.substring(cur.length()-8));
                context.write(keyOut,valueOut);
            }
        }
        userset.clear();
    }
}
