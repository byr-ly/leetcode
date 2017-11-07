package com.eb.bi.rs.andedu.inforec.load_allinfos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.eb.bi.rs.andedu.inforec.get_idf_value.FilterChar;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by LiMingji on 2016/3/18.
 */
public class LoadInfosMapper extends Mapper<Object, Text, Text, Text> {
	
//	private String filterChar;
	private int titleTimes;
	private int contentTimes;
	private String segMethod ;
	@Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\u0001", -1);
        if (fields.length < 4) {
            System.out.println("LoadNewsMapper: Bad Line: " + value.toString());
            return;
        }
        if ("测试".equals(fields[3])) {
        	fields[3] = "";
		}
        String infosID = fields[0];
        String infosTitle = FilterChar.clearNotChinese(fields[2]);
        String infosContent = FilterChar.clearNotChinese(fields[3]);
//        String infosTitle = fields[2].replaceAll(filterChar, " ");
//        infosTitle = infosTitle.replaceAll("[\\pP<>]", " ");
//        String infosContent = fields[3].replaceAll(filterChar, " ");
//        infosContent = infosContent.replaceAll("[\\pP<>]", " ");

        int totalWordNums = 0;
        Map<String, InfosWord> map = SegMore.segMore(segMethod, infosTitle, infosContent, titleTimes, contentTimes);

        StringBuffer values = new StringBuffer("");
        Iterator<String> it = map.keySet().iterator();
        while (it.hasNext()) {
            String word = it.next();
            InfosWord infosWord = map.get(word);

            values.append(infosWord.word);
            values.append(",");
            values.append(infosWord.times);
            values.append("|");
            totalWordNums += infosWord.times;
        }
        context.write(new Text(infosID + "|" + totalWordNums), new Text(values.toString()));
    }
    
    @Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		titleTimes = Integer.parseInt(conf.get("titleTimes", "1"));
		contentTimes = Integer.parseInt(conf.get("contentTimes", "1"));
		segMethod = conf.get("segMethod", "N");
//		filterChar = conf.get("filterChar", "[\\pP��$^=+~|│④Ⅰ1<>\r\n]");
	}
}