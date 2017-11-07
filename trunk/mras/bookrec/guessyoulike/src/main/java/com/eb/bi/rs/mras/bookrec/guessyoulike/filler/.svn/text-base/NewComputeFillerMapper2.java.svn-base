package com.eb.bi.rs.mras.bookrec.guessyoulike.filler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 用户属性信息
 * */
public class NewComputeFillerMapper2 extends Mapper<Object, Text, Text, Text>{
	
	private Map<String, ArrayList<Double>> bookPropertyMap = new HashMap<String, ArrayList<Double>>();
	
	private Map<String, String> bookClassMap;
	private ArrayList<Integer> selectPropertyIdxArr;
	private int classPrefIdx;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		Configuration conf =  context.getConfiguration();

		String selectPropertiesStr = conf.get("select.property.indexes");
		if (selectPropertiesStr != null) {//不配置默认为全部
			selectPropertyIdxArr = new ArrayList<Integer>();
			String[] selectPropertyIdxStrArr = selectPropertiesStr.split(",");
			for (String index : selectPropertyIdxStrArr) {
				selectPropertyIdxArr.add(Integer.parseInt(index));
			}		
		}
		
		//如果没有配置为-1，
		classPrefIdx = conf.getInt("class.pref.index", -1);
		if (classPrefIdx >= 0 ) {
			bookClassMap = new HashMap<String, String>();
		}		
		
		//加载图书信息
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);		
		for(int i = 0; i < localFiles.length; ++i) {
			String line;
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(localFiles[i].toString()));
				while((line = in.readLine()) != null) {/*图书ID|属性1|属性2|xxxx|*/						
					String[] fields = line.split("\\|", -1);
					ArrayList<Double> bookPropertyArr = new ArrayList<Double>();
					if (selectPropertyIdxArr != null) {						
						for (int selectPropertyIdx : selectPropertyIdxArr) {
							bookPropertyArr.add(Double.parseDouble(fields[selectPropertyIdx]));	
						}
					}else {
						for (int j = 1; j < fields.length; j++) {
							if (j != classPrefIdx) {
								bookPropertyArr.add(Double.parseDouble(fields[j]));
							}							
						}						
					}					
					bookPropertyMap.put(fields[0], bookPropertyArr);
					if (classPrefIdx >= 0) {
						bookClassMap.put(fields[0], fields[classPrefIdx]);
					}
				}			
			}finally {
				if(in != null){
					in.close();
				}
			}
		}
	}
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		//用户|名家偏好|新书偏好|分类偏好(分类1:分1,分类2:分2,分类3:分3)
		String[] fields = value.toString().split("\\|",-1);
		
		Iterator<Entry<String, ArrayList<Double>>> iter = bookPropertyMap.entrySet().iterator();
		Entry<String, ArrayList<Double>> entry;
		while (iter.hasNext()) {
			double pref=0;
			
			entry = iter.next();
			String bookid = entry.getKey();
			ArrayList<Double> bookinfo = new ArrayList<Double>(entry.getValue());
		    
		    for(int i = 0;i != bookinfo.size();i++){
		    	pref+=bookinfo.get(i) * Double.parseDouble(fields[i+1]);
		    }
		    
		    pref+=computeClassPref(bookClassMap.get(bookid),fields[classPrefIdx]);
		
		    //key:用户|1;val:2|用户|补白图书|来源(0)|分
			context.write(new Text(fields[0]),new Text("2|"+fields[0]+"|"+bookid+"|0|"+pref));
		}
	}
	
	private double computeClassPref(String class_id,String userclasspref){
		if(!userclasspref.contains(class_id)){/*format:classid:score,classid:score,classid:score*/
			return 0.0;
		}
		else{
			String[] classPrefPairs = userclasspref.split(",");
			for(int i=0;i!=classPrefPairs.length;i++){
				String[] classPref = classPrefPairs[i].split(":");				
				if(classPref[0].equals(class_id)){		
					return Double.parseDouble(classPref[1]);
				}
			}
		}
		return 0.0;
	}
}
