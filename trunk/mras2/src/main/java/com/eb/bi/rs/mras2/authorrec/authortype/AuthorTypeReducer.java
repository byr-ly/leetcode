package com.eb.bi.rs.mras2.authorrec.authortype;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by liyang on 2016/3/8.
 */
public class AuthorTypeReducer extends Reducer<Text,Text,Text,NullWritable> {

    private HashMap<String,String> bookInfo;
    private HashMap<String,Integer> authorBookNum;

    @Override
    public void reduce(Text key,Iterable<Text> values, Context context)
        throws IOException,InterruptedException{
        authorBookNum = new HashMap<String, Integer>();
        for(Text val:values){
            if(bookInfo.containsKey(val.toString())){
                String writer = bookInfo.get(val.toString());
                if(authorBookNum.containsKey(writer)){
                    Integer sumNum = authorBookNum.get(writer);
                    sumNum++;
                    authorBookNum.put(writer,sumNum);
                }
                else {
                    authorBookNum.put(writer,new Integer(1));
                }
            }
        }

        Iterator iter = authorBookNum.entrySet().iterator();
        while(iter.hasNext()){
            Map.Entry entry = (Map.Entry) iter.next();
            String writer = (String) entry.getKey();
            Integer number = authorBookNum.get(writer);
            context.write(new Text(writer + "|" + key + "|" + number), NullWritable.get());
        }
    }

    protected void setup(Context context) throws IOException, InterruptedException {

        bookInfo = new HashMap<String, String>();

        System.out.printf("reduce setup");
        Configuration conf = context.getConfiguration();
        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

        if (localFiles == null) {
            System.out.println("没有找到图书信息File ");
            return;
        }
        for (int i = 0; i < localFiles.length; i++) {
            System.out.println("localFile: " + localFiles[i]);
            String line;
            BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));

            String fileName = localFiles[i].toString();
            if (fileName.contains("0000")) {
                //图书  | 作家
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    if (fields.length < 2) {
                        continue;
                    }
                    String book = fields[0];
                    String writer = fields[1];
                    if (!bookInfo.containsKey(book)) {
                        bookInfo.put(book,writer);
                    }
                }
                br.close();
                System.out.println("图书信息列表加载成功 " + bookInfo.size());
            }
        }
    }
}
