package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.getAndJoinUserInfo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**输入格式：用户msisdn|chrg_dmd_fee点播费用|dnld_cnt下载量|vst_pv访问量(近一周)|chrg_dmd_fee点播费用|dnld_cnt下载量|vst_pv访问量(近一月)
 * 输出：用户msisdn A|chrg_dmd_fee点播费用|dnld_cnt下载量|vst_pv访问量(近一周)|chrg_dmd_fee点播费用|dnld_cnt下载量|vst_pv访问量(近一月)
 * Created by linwanying on 2017/6/23.
 */
public class getUserStatisticsMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyOut = new Text();
    private Text valueOut = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
        if (fields.length < 1) return;
        keyOut.set(fields[0]);
        valueOut.set("A|"+value.toString().substring(fields[0].length()+1));
        context.write(keyOut, valueOut);
    }
}
