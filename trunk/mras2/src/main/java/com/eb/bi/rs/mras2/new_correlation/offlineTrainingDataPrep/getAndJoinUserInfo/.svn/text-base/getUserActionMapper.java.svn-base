package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.getAndJoinUserInfo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.eb.bi.rs.mras2.new_correlation.util.TimeUtil;

import java.io.IOException;

/**获取用户行为数据
 * 输入格式：用户msisdn|book_id图书|read_depth|tele_type号码类型|chrg_dmd_fee点播费用|dnld_cnt下载量|vst_pv访问量|last_read_day最近阅读时间
 * 输出格式：用户msisdn A|chrg_dmd_fee点播费用|dnld_cnt下载量|vst_pv访问量(近一周)
 *          用户msisdn B|chrg_dmd_fee点播费用|dnld_cnt下载量|vst_pv访问量(近一月)
 * Created by linwanying on 2017/6/23.
 */
public class getUserActionMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyOut = new Text();
    private Text valueOut = new Text();
    private String date;

    @Override
    public void setup(Context context) throws IOException,InterruptedException {
        date = TimeUtil.getToday("yyyyMMdd");
//        date = context.getConfiguration().get("date");
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
        if (fields.length < 8) return;
        keyOut.set(fields[0]);
        if (!fields[7].isEmpty()) {
            long timelag = TimeUtil.getTimeMillis(date, "yyyyMMdd") - TimeUtil.getTimeMillis(fields[7], "yyyyMMdd");
            if (timelag <= 7*24*60*60*1000L && timelag >= 0) {
                valueOut.set("A|"+fields[4]+"|"+fields[5]+"|"+fields[6]+"|"+fields[7]);
                context.write(keyOut, valueOut);
            }
            if (timelag <= 30*24*60*60*1000L && timelag >= 0) {
                valueOut.set("B|"+fields[4]+"|"+fields[5]+"|"+fields[6]+"|"+fields[7]);
                context.write(keyOut, valueOut);
            }
        }
    }
}
