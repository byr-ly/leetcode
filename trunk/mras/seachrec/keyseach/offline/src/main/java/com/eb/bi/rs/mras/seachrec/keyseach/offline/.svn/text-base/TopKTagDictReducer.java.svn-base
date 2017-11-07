package com.eb.bi.rs.mras.seachrec.keyseach.offline;
import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKTagDictReducer extends Reducer<Text, TagDictWritable, Text, NullWritable>{	
	
	int k = 5;	
	
	protected void reduce(Text key, Iterable<TagDictWritable> tagDicts, Context context) throws IOException ,InterruptedException {

		TreeSet<TagDictWritable> tagDictSet = new TreeSet<TagDictWritable>();

		for(TagDictWritable tagDict: tagDicts){
			//因为是变量tagDict是不可重用的，一定要new一个对象
			tagDictSet.add(new TagDictWritable(tagDict.getTag(),tagDict.getWeight()));
			if(tagDictSet.size() > k){
				tagDictSet.remove(tagDictSet.first());
			}
		}
		int sum = 0;
		for(TagDictWritable tagDict:tagDictSet){
			sum += tagDict.getWeight();					
		}
		double NormlizedWeight = 1.0;
		for(TagDictWritable tagDict:tagDictSet){			
			NormlizedWeight = 1.0 * tagDict.getWeight() / sum;
			Text outkeyText = new Text(key + "|" + tagDict.getTag()+ "|" + NormlizedWeight);			
			context.write(outkeyText,NullWritable.get());	
		}
	}
	
	protected void setup(Context context) throws IOException ,InterruptedException {
		super.setup(context);
		k = context.getConfiguration().getInt("topk.number", 5);
	}
}
