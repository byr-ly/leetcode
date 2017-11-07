package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.recClick;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import com.eb.bi.rs.mras2.new_correlation.util.TimeUtil;

import java.io.IOException;

/**获取推荐点击结果mapper
 * 输入格式：2017-06-10 00:00:01,113  INFO token=8ec004c9da0f4f&msisdn=30310887551&ctype=CMREADBC_Android_WH_V6.87_170527&page_id=1&prtype=9&page_booknum=9&bid=447892651&_=1497024001655&jsonpcallback=jsonpback279721   350953787|362460611|346866566|441685912|452774053|400692948|410686209|405274596|601659376|
 * 格式：用户 A|源图书|推荐图书|时间
 * Created by linwanying on 2017/6/9.
 */
public class getRecListMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyOut = new Text();
    private Text valueOut = new Text();
    private Logger log = Logger.getLogger("getRecListMapper");

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        if (fields.length <= 4) return;
        String[] info = fields[4].split("&");
        StringBuffer sb = new StringBuffer();
        if (info.length > 6) {
            String[] prtype = info[4].split("=");
            String[] bid = info[6].split("=");
            if (prtype.length == 2 && prtype[0].trim().equals("prtype") && prtype[1].trim().equals("9") &&
                    bid.length == 2 && bid[0].trim().equals("bid")) {
                sb.append("A|").append(info[6].split("=")[1]).append("|");
                if (fields.length > 5) {
                    sb.append(fields[fields.length-1]);
                } else {
                    if (fields[4].split("\t").length != 2) return;
                    sb.append(fields[4].split("\t")[1]);
                }
                sb.append(TimeUtil.getTimeString(TimeUtil.getTimeMillis(fields[0], "yyyy-MM-dd")));
                keyOut.set(info[1].split("=")[1]);
                valueOut.set(sb.toString());
                log.info(keyOut+" "+valueOut);
                context.write(keyOut, valueOut);
            }
        }
    }
}
