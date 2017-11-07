package com.eb.bi.rs.mras2.bookrec.qiangfarec.selectbooks;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;


public class SelectHighlyRecommendBooksReducer extends Reducer<Text, Text, Text, NullWritable>  {
	private Map<String,List<String>> class_bookInfo = new HashMap<String,List<String>>();
	private Map<String,List<String>> department_bookInfo = new HashMap<String,List<String>>();
	private Map<String,String> class_department = new HashMap<String,String>();
	private Map<String,List<String>> zone_books = new HashMap<String,List<String>>();
	protected static String HIGHLYREC = "highlyRec";
	protected static String TOPREC = "topRec";
	private int highlyRecBookNum;
	private int topBookNumber;
	private Set<String> zone_ids = new HashSet<String>();
	private MultipleOutputs mos;

	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException ,InterruptedException {
		for (Text text : values) {
			String[] fields = text.toString().split("\\|",-1);//前三分类 |相似分类 |历史阅读分类
			for (String zone_id : zone_ids) {
				
				Set<String> books = new HashSet<String>();
				for(String string : fields){
					if(books.size() >= highlyRecBookNum){
						break;
					}
					if(StringUtils.isBlank(string)){
						continue;
					}
					books = getBooks(class_bookInfo , string , zone_id , books);
				}
				
				if (books.size() < highlyRecBookNum) {
					for(String string : fields){
						if(books.size() >= highlyRecBookNum){
							break;
						}
						if(StringUtils.isBlank(string)){
							continue;
						}
						String[] class_ids = string.split(",");
						
						for (String class_id : class_ids) {
							books = getBooks(department_bookInfo,class_department.get(class_id) , zone_id , books);	
						}
					}
				}
				
				if (books.size() < highlyRecBookNum) {
					books = getBooks(zone_books, zone_id , zone_id , books);
				}
				
				
				write(context,key.toString() + "_" + zone_id , books , HIGHLYREC);
				
				if (books.size() > topBookNumber) {
					books = chooseBooks(books , topBookNumber,  new HashSet<String>());
				}
				write(context,key.toString() + "_" + zone_id , books , TOPREC);
									
			}
		}				
		
	}

	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();		
		String bookClassifiedInfo = conf.get("books.classify.info");
		String classMapdepartment = conf.get("class.department");
		mos = new MultipleOutputs(context);
		topBookNumber = Integer.parseInt(conf.get("top.book.number"));
		highlyRecBookNum = Integer.parseInt(conf.get("highly.recommend.book.number"));
		//Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		URI[] localFiles = context.getCacheFiles();
		for(int i = 0; i < localFiles.length; ++i) {
			String line;
			BufferedReader in = null;
			try {
				Path path = new Path(localFiles[i].getPath());
				in = new BufferedReader(new FileReader(path.getName().toString()));
				if (path.toString().contains(bookClassifiedInfo)) {
					while((line = in.readLine()) != null) { //专区id|bookid|事业部ID|分类Class_id|
						String[] fields = line.split("\\|");
						if (fields.length == 4 && !StringUtils.isBlank(fields[0]) && !StringUtils.isBlank(fields[1])) {
							zone_ids.add(fields[0]);
							
							List<String> bookList = zone_books.get(fields[0]+ "|" + fields[0]);
							if (bookList == null) {
								bookList = new ArrayList<String>();
								bookList.add(fields[1]);
								zone_books.put( fields[0]+ "|" + fields[0], bookList);
							} else {
								bookList.add(fields[1]);
							}
							
							List<String> books = department_bookInfo.get(fields[2] + "|" + fields[0]);
							if (books == null) {
								books = new ArrayList<String>();
								books.add(fields[1]);
								department_bookInfo.put(fields[2] + "|" + fields[0], books);
							} else {
								books.add(fields[1]);
							}
							
							books = class_bookInfo.get(fields[3] + "|" + fields[0]);
							if (books == null) {
								books = new ArrayList<String>();
								books.add(fields[1]);
								class_bookInfo.put(fields[3] + "|" + fields[0], books);
							} else {
								books.add(fields[1]);
							}
						}					
					}								
				}else if (path.toString().contains(classMapdepartment)) {
					while((line = in.readLine()) != null) { //分类id|bu_type1,bu_type2,bu_type3
						String[] fields = line.split("\\|", -1);
						if (fields.length == 2) {
							class_department.put(fields[0], fields[1]);
						}					
					}				
				}
			}finally {
				if(in != null){
					in.close();
				}
			}			
		}
	}
	
	
	private Set<String> getBooks(Map<String , List<String>> map , String str , String zone_id , Set<String> books) throws IOException ,InterruptedException {
		if (StringUtils.isBlank(str)) {
			return books;
		}
		
		Set <String> bookSet = new HashSet<String>();
		String[] keys = str.split(",");
		for (int i = 0; i < keys.length; i++) {
			List<String> list = map.get(keys[i] + "|" + zone_id);
			if (list != null) {
				bookSet.addAll(list);				
			}
		}
		
		int size = books.size();
		int bookSize = bookSet.size();
		int num = highlyRecBookNum - size;
		if (bookSize <= num) {
			books.addAll(bookSet);
		} else {
				books = chooseBooks(bookSet , num , books);
		}
		
		return books;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Set<String> chooseBooks(Set<String> set, int num,Set<String> s) {
		
		Set result = s;
		Object[] books = set.toArray();
		int size = books.length;
		while (num > 0){			
			Random r = new Random(System.currentTimeMillis());
			int n = r.nextInt(size);
			for(int i = 0 ; i < set.size() && result.contains(books[n]) ; i++){
				n = (n+1)%size;	
			}
			result.add(books[n]);
			num--;
		}
		
		return result;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void write(Context context , String key, Set set , String path) throws IOException, InterruptedException {
		String result = "";
		Iterator<String> it = set.iterator();
		while (it.hasNext()) {
			result += it.next() + "|";
		}
		mos.write(path, new Text(key + ";" + result),NullWritable.get(),"zone_" + path + "/part");
	}
	
	@Override	
	public void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
