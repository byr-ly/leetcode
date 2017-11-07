package com.eb.bi.rs.mras.authorrec.itemcf.filler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FillerUserFilterMapper extends Mapper<Object, Text, Text, Text> {
    private Set<String> predictUsers = new HashSet<String>();
    private Map<String, List<String>> userPreferClassMap
            = new HashMap<String, List<String>>();

    /**
     * @param value: 输入：待推荐用户
     *               格式：msisdn
     *               关联偏好表：msisdn|第n偏好|偏好classid|authorid1|score1|author2|score2|...
     *               map out:
     *               key:msisdn, value: 第n偏好|偏好classid
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        if (predictUsers.contains(msisdn)) {
            return;
        }
        List<String> prefClasses = userPreferClassMap.get(msisdn);
        String outStr = "-1|";
        context.write(new Text(msisdn), new Text(outStr));
        if (prefClasses == null) {
            return;
        }
        for (int i = 0; i < prefClasses.size(); i++) {
            if (prefClasses.get(i).equalsIgnoreCase("null")
                    || prefClasses.get(i).equalsIgnoreCase("\\N")) {
                continue;
            }
            String classid = prefClasses.get(i);
            outStr = String.format("%d|%s", i + 1, classid);
            context.write(new Text(msisdn), new Text(outStr));
        }
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        JobExecuUtil execuUtil = new JobExecuUtil();
        URI[] localFiles = execuUtil.getCacheFiles(context);
        String predictRecommOut = context.getConfiguration().get(
                PluginUtil.TEMP_NO_FILLER_USER_KEY);
        predictRecommOut = execuUtil.getFileName(predictRecommOut);
        String prefPath = context.getConfiguration()
                .get(PluginUtil.TEMP_TORECOMM_USER_PREF_FILTER_KEY);
        prefPath = execuUtil.getFileName(prefPath);
        System.out.println(predictRecommOut);
        //格式：msisdn
        for (URI path : localFiles) {
            if (!path.toString().contains(predictRecommOut)) {
                continue;
            }
            System.out.println(path.toString());
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String msisdn = fields[0];
                    predictUsers.add(msisdn);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        String logStr = String.format("FillerUserFilterMapper set up "
                + "predictUsers size %d", predictUsers.size());
        System.out.println(logStr);
        //用户偏好
        //格式：msisdn|class1_id|score1|class2_id|score2|class3_id|score3
        System.out.println(prefPath);
        for (URI path : localFiles) {
            if (!path.toString().contains(prefPath)) {
                continue;
            }
            System.out.println(path.toString());
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String msisdn = fields[0];
                    String classid1 = fields[1];
                    String classid2 = fields[3];
                    String classid3 = fields[5];
                    List<String> prefClasses = new ArrayList<String>();
                    prefClasses.add(classid1);
                    prefClasses.add(classid2);
                    prefClasses.add(classid3);
                    userPreferClassMap.put(msisdn, prefClasses);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        logStr = String.format("FillerUserFilterMapper set up "
                + "userPreferClassMap size %d", userPreferClassMap.size());
        System.out.println(logStr);
    }

}
