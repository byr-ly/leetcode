package com.eb.bi.rs.andedu.inforec.get_keyword_infos;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by LiMingji on 2016/4/12.
 */
public class GetKeyWordInfosReducer extends Reducer<Text, Text, Text, Text> {
    /**
     * @param key
     * @param values value的格式为：
     *               newsID|classID word,weight|word,weight....
     *               输入数据为：
     *               KEY:word|classID
     *               VALUE:newsID,weight|.....
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer("");
        for (Text value : values) {
            sb.append(value.toString());
            sb.append("|");
        }
        context.write(key, new Text(sb.toString()));
    }
}
