package com.eb.bi.rs.andedu.inforec.getRetsAndFilter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiMingji on 2016/3/21.
 */
public class GetRetsAndFilterMapper extends Mapper<Object, Text, Text, Text> {

    /**
     * 输入数据
     *   key:    word|classID
     *   value : newsID1,newsID2,weight1,weight2
     * 输出数据
     *   key:   newsID|classID
     *   value: newsID|scores1|scores2
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] keyFields = value.toString().split("\t");
        if (keyFields.length < 2) {
            System.out.println("Bad line ! " + value.toString() + " " + keyFields.length);
            return;
        }
        String word = keyFields[0];

        String[] valueFields = keyFields[1].split("\\|");
        if (valueFields.length < 4) {
            System.out.println("Bad Value line ! " + keyFields[1]);
        }
        String newsID = valueFields[0];
        String compNewsID = valueFields[1];
        String scores1 = valueFields[2];
        String scores2 = valueFields[3];

        context.write(new Text(newsID), new Text(compNewsID + "|" + scores1 + "|" + scores2));
    }
}