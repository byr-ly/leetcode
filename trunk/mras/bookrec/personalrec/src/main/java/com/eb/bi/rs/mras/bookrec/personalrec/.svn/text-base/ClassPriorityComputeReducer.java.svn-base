package com.eb.bi.rs.mras.bookrec.personalrec;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ClassPriorityComputeReducer extends Reducer<Text,Text, Text, Text> {
	private Map<String,String> class3toclass2 = new HashMap<String,String>();
	
	private Set<String> classList = new HashSet<String>();
	
	private String ifhaveunuseClass;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		ifhaveunuseClass = context.getConfiguration().get("Appconf.if.have.unuse.class");
		
		super.setup(context);
		Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		
		for(int i = 0; i < localFiles.length; i++){
			String line;
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(localFiles[i].toString()));
				
				if(localFiles[i].toString().contains("class_list")) {
					while ((line = in.readLine()) != null) {					
						String fields[] = line.split("\\|");
						classList.add(fields[0]);
					}
				}
				else{
					while ((line = in.readLine()) != null) {					
						String fields[] = line.split("\\|");
						class3toclass2.put(fields[0], fields[1]);
					}
				}

			} finally {
				if (in != null) {
					in.close();
				}			
			}			
		}	
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Set<String> bigclassSet = new HashSet<String>();
		Set<String> mainClass3Set = new HashSet<String>();
		
		Set<String> useClass3Set = new HashSet<String>();
		
		for(Text value:values){
			context.write(key,new Text(value.toString() + "|2"));
			bigclassSet.add(class3toclass2.get(value.toString()));
			mainClass3Set.add(value.toString());
			
			useClass3Set.add(value.toString());
		}
			
		String keyC = "";
		String valC = "";
		
		for(String bigClass:bigclassSet){
			Iterator<Entry<String, String>> it = class3toclass2.entrySet().iterator();
			while (it.hasNext()){
				Map.Entry<String, String> entryC = (Map.Entry<String, String>) it.next();
				keyC = entryC.getKey();
				valC = entryC.getValue();
				
				if(mainClass3Set.contains(keyC))
					continue;
				
				if(valC.equals(bigClass)){
					context.write(key,new Text(keyC + "|1"));
					
					useClass3Set.add(keyC);
				}
			}
		}
		
		if(ifhaveunuseClass.equals("yes")){
			for(String unuseClass:classList){
				if(!useClass3Set.contains(unuseClass)){
					context.write(key,new Text(unuseClass + "|0"));
				}
			}
		}
		
	}
}
