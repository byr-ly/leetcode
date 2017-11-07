package com.eb.bi.rs.mras.andnewsrec.compare_result;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiMingji on 2016/4/18.
 */
public class CompareResultMapperToday extends Mapper<Object, Text, Text, Text> {

    private String removeScores(String input) {
        StringBuffer sb = new StringBuffer("");
        String[] fields = input.split("\\|", -1);
        for (String field : fields) {
            if (field.isEmpty()) {
                continue;
            }
            String newsID = field.split(",")[0];
            sb.append(newsID);
            sb.append("|");
        }
        if (sb.length() > 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length < 2) {
            return;
        }
        String newsIDAndClassification = fields[0];
        String simiNewsAndScores = "today@" + removeScores(fields[1]);
        context.write(new Text(newsIDAndClassification), new Text(simiNewsAndScores));
    }
}









