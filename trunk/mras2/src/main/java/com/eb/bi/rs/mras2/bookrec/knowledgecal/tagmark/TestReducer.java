package com.eb.bi.rs.mras2.bookrec.knowledgecal.tagmark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TestReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, String> booktabelMap = new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //加载图书标签集数据

        super.setup(context);
//		Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        URI[] localFiles = context.getCacheFiles();

        for (int i = 0; i < localFiles.length; i++) {
            String line;
            BufferedReader in = null;
            try {
                Path path = new Path(localFiles[i]);
                in = new BufferedReader(new FileReader(path.getName().toString()));
                while ((line = in.readLine()) != null) {
                    String fields[] = line.split("\\|");
                    booktabelMap.put(fields[0], fields[1]);
                }
            } finally {
                if (in != null) {
                    in.close();
                }
            }
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Float> Label_score = new HashMap<String, Float>();

        float total_booknum = 0;

        for (Text value : values) {//用户阅读图书list
            total_booknum++;

            String field = value.toString();

            if (!booktabelMap.containsKey(field)) {
                //context.write(key,value);//测试用没有找到的书
                continue;
            }

            String labelString = booktabelMap.get(field);

//			context.write(key,new Text(value.toString()+"|"+labelString));//测试用

			/*
			String[] labels = labelString.split(";");
			
			for(int i = 0;i != labels.length;i++){
				if(labels[i].equals(""))
					continue;
				
				if(Label_score.containsKey(labels[i])){
					Label_score.put(labels[i], Label_score.get(labels[i])+1);
				}
				else{
					Label_score.put(labels[i], (float) 1);
				}
			}	
			 */
        }
		/*
		String keyL = "";
		float valL = 0;
		
		float LabelScore = 0;
		
		Iterator<Entry<String, Float>> iter = Label_score.entrySet().iterator();
		while (iter.hasNext()){
			Map.Entry<String, Float> entryL = (Map.Entry<String, Float>) iter.next();
			keyL = entryL.getKey();
			valL = entryL.getValue();
			
			LabelScore = valL / total_booknum;
			
			context.write(key,new Text(keyL+"|"+LabelScore));
		}
		*/
    }

    protected void cleanup(Context context
    ) throws IOException, InterruptedException {
        String keyL = "";
        String valL = "";

        Iterator<Entry<String, String>> iter = booktabelMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entryL = (Map.Entry<String, String>) iter.next();
            keyL = entryL.getKey();
            valL = entryL.getValue();

            context.write(new Text(keyL), new Text(valL.split(";").toString()));
        }
    }
}
