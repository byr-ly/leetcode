package com.eb.bi.rs.mras2.bookrec.qiangfarec.sortofzone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;

public class UserBookTotalscoreReducer extends Reducer<Text, Text, NullWritable, Text> {

	private String fieldDelimiter;
	private String fieldDelimiter1;
	private double hisBookPoints;
	private int cacheInfoLength;
	
	private Map<String, Map<String, Double>> sortOfZonePoints = new HashMap<String, Map<String, Double>>();

	@SuppressWarnings("resource")
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		fieldDelimiter = conf.get("field.delimiter", "\\|");
		fieldDelimiter1 = conf.get("field.delimiter1", ",");
		hisBookPoints = Double.parseDouble(conf.get("his.book.point", "0.1"));
		cacheInfoLength = Integer.parseInt(conf.get("cache.info.length", "3"));

		//Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
		URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles == null) {
            //System.out.println("local cacheFiles File is null");
            return;
        }
		for (int i = 0; i < cacheFiles.length; i++) {
            String line;
			Path path = new Path(cacheFiles[i].getPath());
            BufferedReader br = new BufferedReader(new FileReader(path.getName().toString()));
            while ((line = br.readLine()) != null) {
            	String[] cacheStrs = line.split(fieldDelimiter);
				if (cacheStrs.length < cacheInfoLength) {
					continue;
				}
				String area = cacheStrs[0];
				String bookid = cacheStrs[1];
				double editScore = Double.parseDouble(cacheStrs[2]);
				Map<String, Double> bookPoints;
				if(sortOfZonePoints.containsKey(area)){
					bookPoints = sortOfZonePoints.get(area);
					bookPoints.put(bookid, editScore);
				} else {
					bookPoints = new HashMap<String, Double>();
					bookPoints.put(bookid, editScore);
				}
				
				sortOfZonePoints.put(area, bookPoints);
            }
        }
	}

	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Map<String, Double>  userBookTotalScore = new HashMap<String, Double>();
		Map<String, String> hisBookId = new HashMap<String, String>();
		for (Text pair : values) {
			String[] fields = pair.toString().split(fieldDelimiter);

			if(fields[0].equals("his")){//用户历史图书list
				hisBookId.put(fields[1], "");
			}
			if(fields[0].equals("pre")){//用户偏好分map， key = bookid, value = prefer_score

				for(int i = 0; i < fields.length; i++){
					//System.out.println("book info is " + pair.toString() );
					String[] book_prefer_socres = fields[i].toString().split(fieldDelimiter1);

					if(book_prefer_socres.length == 2){
						userBookTotalScore.put(book_prefer_socres[0], Double.parseDouble(book_prefer_socres[1]));
					}
				}
			}
		}

		//System.out.println("user is " + key.toString() );


		Iterator<Entry<String, Map<String, Double>>> it = sortOfZonePoints.entrySet().iterator();
		while (it.hasNext()) {

			Entry<String, Map<String, Double>> en = it.next();

			Map<String, Double> resultScore = new HashMap<String, Double>();

			String areaID = en.getKey();
			Map<String, Double> bookIDsPoints = en.getValue();

			Iterator<Entry<String, Double>> iterator = bookIDsPoints.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<String, Double> entry = iterator.next();
				String bookid = entry.getKey();
				double totalScore = entry.getValue();//图书分

				if( userBookTotalScore.get( bookid ) != null && userBookTotalScore.containsKey( bookid ) ) { // 图书存在偏好分
					//System.out.println("bookIDsPoints bookid is " + bookid);
					totalScore += userBookTotalScore.get(bookid);// 在专区图书存在，偏好分 + 编辑分
				}
				if (hisBookId.containsKey(bookid)){ // 在历史图书中存在，再加上反馈分
					totalScore -= hisBookPoints;
				}
				DecimalFormat df = new DecimalFormat("#0.0000"); //保留4位小数
				resultScore.put(bookid, Double.parseDouble(df.format(totalScore)));
			}

//			Iterator<Map.Entry<String, Double>> aa = resultScore.entrySet().iterator();
//			while (aa.hasNext()) {
//				Map.Entry<String, Double> bb = aa.next();
//
//				System.out.println("resultScore bookid is " + bb.getKey());
//			}

	        //图书总分排序
	        List<Entry<String, Double>> list = new ArrayList<Entry<String, Double>>(resultScore.entrySet());

	        if(list.size() != 0){
	            Collections.sort(list, new Comparator<Entry<String, Double>>() {
	                //降序排序  
	                public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {  
	                    //return o1.getValue().compareTo(o2.getValue());  
	                    return o2.getValue().compareTo(o1.getValue());  
	                }
	            });	
	            
	            // 编辑输出 msisdn|专区ID|bookid1 ,bookid2 ,book3
	            String output = key + "|" + areaID + "|";
	            for (int i = 0; i < list.size(); i++) {  
	                Entry<String,Double> ent=list.get(i);  
	                
	                //output +=  "(" + ent.getValue() + ")";
	                output += ent.getKey();
	                
	                if( i != list.size() - 1){
	                	output += ",";
	                }
	            }
	            
	            context.write(NullWritable.get(), new Text(output));
	        }
		}
	}
}
