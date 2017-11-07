package com.eb.bi.rs.mras.authorrec.itemcf.filler;

import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class FamousAuthorBookFilterMapper
        extends Mapper<Object, Text, Text, NullWritable> {
    private Set<String> recommAuthors = new HashSet<String>();

    /**
     * @param value: bookinfo
     *               格式：bookid|authorid|classid|contentstatus|author_grade|typeid
     *               筛选:1,type=1，contentstatus=13
     *               2,作者为推荐库关联用户
     *               3，作者为名家
     *               map out:
     *               key:
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String authorid = strs[1];
        String typeid = strs[4];
        String status = strs[3];
        if (!typeid.equals("1")) {
            return;
        }
        if (!status.equals("13")) {
            return;
        }
        if (!recommAuthors.contains(authorid)) {
            return;
        }
        context.write(value, NullWritable.get());
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        JobExecuUtil execuUtil = new JobExecuUtil();
        URI[] localFiles = execuUtil.getCacheFiles(context);
        String authorClassTabPath = context.getConfiguration().get(PluginUtil.TEMP_AUTHOR_CLASSIFY_TABLE_KEY);

        String famousAuthorPath = context.getConfiguration().get(PluginUtil.FAMOUS_AUTHOR_KEY);
        Set<String> famousAuthors = new HashSet<String>();
        for (URI path : localFiles) {
            if (!path.toString().contains(famousAuthorPath)) {
                continue;
            }
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(path, context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String authorid = fields[0];
                    famousAuthors.add(authorid);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        //格式：筛选后authorid|classid|count
        for (URI path : localFiles) {
            if (!path.toString().contains(authorClassTabPath)) {
                continue;
            }
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(path, context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String authorid = fields[0];
                    if (!famousAuthors.contains(authorid)) {
                        continue;
                    }
                    recommAuthors.add(authorid);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
    }
}
