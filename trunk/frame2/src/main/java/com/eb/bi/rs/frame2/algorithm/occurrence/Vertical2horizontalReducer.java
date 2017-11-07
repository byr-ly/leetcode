package com.eb.bi.rs.frame2.algorithm.occurrence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Vertical2horizontalReducer extends Reducer<Text,Text, Text, Text> {
	
	private static String m_separator;
	
	private static int m_neighbourNum;
	
	private static int m_maxitemNum;
	
	protected void setup(Context context) throws IOException,InterruptedException {
		m_separator = context.getConfiguration().get("id_id_separator");
		
		m_neighbourNum = Integer.valueOf(context.getConfiguration().get("neighbour_num")).intValue();
	
		m_maxitemNum = Integer.valueOf(context.getConfiguration().get("max_item_num")).intValue();
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		String bookIdString = new String();
		List<String> ids = new ArrayList<String>();
		Set oknumSet = new HashSet();
		
		for (Text value : values){
			ids.add(value.toString());
			if(ids.size()>=m_maxitemNum)
				return;
		}
		
	
		
		if(ids.size()>=1){
			Collections.sort(ids);
		
			if(ids.size()>m_neighbourNum && m_neighbourNum>1){
				oknumSet = generateRandom(m_neighbourNum,ids.size());
				for(int i = 0; i != ids.size(); i++){
					if(oknumSet.contains(i))
						bookIdString+=ids.get(i)+m_separator;
				}
			}
			else{
				for(int i = 0; i != ids.size(); i++){
					bookIdString+=ids.get(i)+m_separator;
				}
			}
		
			bookIdString = bookIdString.substring(0,bookIdString.length()-m_separator.length());
			context.write(key, new Text(bookIdString));
		}
	}
	
	public Set generateRandom(int neighbourNum, int size){
		Set list = new HashSet();
        
        int n = size;  
        Random rand = new Random();  
          
        int num =0;  
          
        while(true){  
        	num = rand.nextInt(n);
        	list.add(num);
        	
        	if(list.size()>neighbourNum)
        		break;
        }
        
        return list;
	}
}
