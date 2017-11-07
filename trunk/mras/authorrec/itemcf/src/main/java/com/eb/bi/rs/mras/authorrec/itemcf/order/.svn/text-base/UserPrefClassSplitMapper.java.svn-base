package com.eb.bi.rs.mras.authorrec.itemcf.order;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class UserPrefClassSplitMapper extends Mapper<Object, Text, Text, Text> {
    private Set<String> deepUserSet = new HashSet<String>();
    /**
     * @param value:
     * map out:
     * key: ; value:
     */
    private JobExecuUtil execuUtil = new JobExecuUtil();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        String prefStrs = value.toString().substring(msisdn.length() + 1);
        if (!deepUserSet.contains(msisdn)) {
            return;
        }
        context.write(new Text(msisdn), new Text(prefStrs));
    }


    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        URI[] localFiles = execuUtil.getCacheFiles(context);
        for (URI path : localFiles) {
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String user = fields[0];
                    deepUserSet.add(user);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
    }

}
