package com.eb.bi.rs.andedu.predictScore;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 计算用户预测topN评分的图书
 * 输出格式：用户；品牌1，评分1|品牌2，评分2|...
 */
public class TopUserPredictBooksReducer extends Reducer<Text, Text, Text, NullWritable> {
	private int topN;
	//存放推荐品牌
    private TreeSet<String> tops;
	private Comparator<String> topNComp = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            String[] fields1 = o1.split(",");
            String bookID1 = fields1[0];
            Double scores1 = Double.parseDouble(fields1[1]);

            String[] fields2 = o2.split(",");
            String bookID2 = fields2[0];
            Double scores2 = Double.parseDouble(fields2[1]);

            if (bookID1.equals(bookID2)) {
                return 0;
            }
            if (scores1 > scores2) {
                return -1;
            }else {
                return 1;
            }
        }
    };
    
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
		HashSet<String>  hisBrandSet = new HashSet<String>();
		Map<String,String>  recMap = new HashMap<String,String>();
		hisBrandSet.clear();recMap.clear();
		for (Text value : values) {
			String fields[] = value.toString().split("\\|");
			if(fields[0].equals("0")){
				for(int i=2;i<fields.length-1;i+=2){
					hisBrandSet.add(fields[i]);
				}
			}else if(fields[0].equals("1")){
				//DecimalFormat dcmFmt = new DecimalFormat("0.0000");
				//recMap.put(fields[2], fields[2]+","+dcmFmt.format(Double.parseDouble(fields[3])));
				recMap.put(fields[2], fields[2]+","+Double.parseDouble(fields[3]));//,
			}
		}
		
		tops = new TreeSet<String>(topNComp);
		tops.clear();
		for (Map.Entry<String,String> entry :recMap.entrySet()) {
			if(!hisBrandSet.contains(entry.getKey())){
				tops.add(entry.getValue());
				if (tops.size() > topN) {
                    tops.remove(tops.last());
                }
				
			}
		}
		StringBuffer sb = new StringBuffer(key.toString() + "\t");   //用户;
        for (String value : tops) {
            sb.append(value);
            sb.append("|");
            
        }
        if(tops.size()!=0){
        	context.write(new Text(sb.toString()), NullWritable.get());
        }
	}
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		topN =  Integer.parseInt(conf.get("conf.top.userpredictscore.num", "50"));
	}
}
