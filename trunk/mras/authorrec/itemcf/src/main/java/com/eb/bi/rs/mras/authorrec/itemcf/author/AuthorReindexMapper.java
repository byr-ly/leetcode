package com.eb.bi.rs.mras.authorrec.itemcf.author;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AuthorReindexMapper
        extends Mapper<Object, Text, Text, NullWritable> {
    private Map<String, Integer> authorIndexMap = new HashMap<String, Integer>();

    /**
     * @param value: 用户-平均分
     *               格式：authorid|score
     *               map out:
     *               key:idx|authorid|score
     */
    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String authorid = strs[0];
        int idx = authorIndexMap.get(authorid);
        String keyStr = String.format("%d|%s", idx, value.toString());
        context.write(new Text(keyStr), NullWritable.get());
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        JobExecuUtil execuUtil = new JobExecuUtil();
        URI[] localFiles = execuUtil.getCacheFiles(context);
        List<String> authors = new ArrayList<String>();
        //格式：authorid|avg
        for (URI path : localFiles) {
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String authorid = fields[0];
                    authors.add(authorid);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        Collections.sort(authors);
        for (int i = 0; i < authors.size(); i++) {
            String author = authors.get(i);
            authorIndexMap.put(author, i);
        }
    }
}
