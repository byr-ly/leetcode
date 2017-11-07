package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.getAndJoinUserInfo;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**获取用户统计行为
 * 输入数据：用户msisdn A|chrg_dmd_fee点播费用|dnld_cnt下载量|vst_pv访问量(近一周)
 *          用户msisdn B|chrg_dmd_fee点播费用|dnld_cnt下载量|vst_pv访问量(近一月)
 * 输出数据：用户msisdn|chrg_dmd_fee点播费用|dnld_cnt下载量|vst_pv访问量(近一周)|chrg_dmd_fee点播费用|dnld_cnt下载量|vst_pv访问量(近一月)
 * Created by linwanying on 2017/6/23.
 */
public class getUserActionReducer extends Reducer<Text, Text, Text, NullWritable> {
    private Text keyOut = new Text();
    private NullWritable valueOut = NullWritable.get();
    private Logger log = Logger.getLogger("getUserActionReducer");

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double feeWeekly = 0, feeMonthly = 0, dnldWeekly = 0, dnldMonthly = 0, vstpvWeekly = 0, vstpvMonthly = 0;
        for (Text value : values) {
            String[] fields = value.toString().split("\\|");
            if (fields[0].equals("A")) {
                feeWeekly += Double.parseDouble(fields[1].isEmpty()?"0":fields[1]);
                dnldWeekly += Double.parseDouble(fields[2].isEmpty()?"0":fields[2]);
                vstpvWeekly += Double.parseDouble(fields[3].isEmpty()?"0":fields[3]);
            }
            if (fields[0].equals("B")) {
                feeMonthly += Double.parseDouble(fields[1].isEmpty()?"0":fields[1]);
                dnldMonthly += Double.parseDouble(fields[2].isEmpty()?"0":fields[2]);
                vstpvMonthly += Double.parseDouble(fields[3].isEmpty()?"0":fields[3]);
            }
        }
        keyOut.set(key+"|"+feeWeekly+"|"+dnldWeekly+"|"+vstpvWeekly+"|"+feeMonthly+"|"+dnldMonthly+"|"+vstpvMonthly);
        context.write(keyOut, valueOut);
    }
}
