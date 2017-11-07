package com.eb.bi.rs.mras2.authorrec.pre_author;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by liyang on 2016/3/9.
 */
public class PreAuthorReducer extends Reducer<Text, Text, Text, NullWritable> {

    Logger log = Logger.getLogger(PreAuthorReducer.class);

    private ArrayList<String> authorAllList;
    private ArrayList<String> authorCopyList;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<String> authorList = new ArrayList<String>();
        authorCopyList = new ArrayList<String>(authorAllList);
        for (Text val : values) {
            authorList.add(val.toString());
        }
        authorCopyList.removeAll(authorList);
        for (int i = 0; i < authorCopyList.size(); i++) {
            context.write(new Text(key + "|" + authorCopyList.get(i)), NullWritable.get());
        }
    }

    protected void setup(Context context) throws IOException, InterruptedException {

        authorAllList = new ArrayList<String>();

        System.out.printf("reduce setup");
        Configuration conf = context.getConfiguration();
        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

        if (localFiles == null) {
            System.out.println("没有找到作家信息File ");
            return;
        }
        for (int i = 0; i < localFiles.length; i++) {
            System.out.println("localFile: " + localFiles[i]);
            String line;
            BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));

            String fileName = localFiles[i].toString();
            if (fileName.contains("author")) {
                //作家
                while ((line = br.readLine()) != null) {
                    String writer = line;
                    authorAllList.add(writer);
                }
                br.close();
                System.out.println("作家信息加载成功 " + authorAllList.size());
            }
        }
    }
}
