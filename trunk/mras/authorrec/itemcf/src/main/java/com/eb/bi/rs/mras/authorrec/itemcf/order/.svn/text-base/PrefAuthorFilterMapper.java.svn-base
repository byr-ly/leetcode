package com.eb.bi.rs.mras.authorrec.itemcf.order;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.ScoreWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PrefAuthorFilterMapper
        extends Mapper<Object, Text, Text, ScoreWritable> {

    private Map<String, List<String>> userPreferClassMap
            = new HashMap<String, List<String>>();
    private Map<String, List<String>> authorClassMap
            = new HashMap<String, List<String>>();

    /**
     * @param value: 格式：msisdn|predictAuthorId|predictScore
     *               map out:
     *               key:msisdn|第n偏好|偏好classid, value:predictAuthorId|predictScore
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        String authorId = strs[1];
        double score = Double.parseDouble(strs[2]);
        boolean bClassMatch = false;
        List<String> prefs = userPreferClassMap.get(msisdn);
        List<String> aClasses = authorClassMap.get(authorId);
        ScoreWritable scoreWritable = new ScoreWritable(authorId, score);
        if (prefs != null && aClasses != null) {
            for (int i = 0; i < prefs.size(); i++) {
                if (prefs.get(i) == null) {
                    continue;
                }
                if (aClasses.contains(prefs.get(i))) {
                    bClassMatch = true;
                    String mapKey = String.format("%s|%d|%s",
                            msisdn, (i + 1), prefs.get(i));
                    context.write(new Text(mapKey), scoreWritable);
                }
            }
        }
        if (!bClassMatch) {
            String mapKey = String.format("%s|%d|%s", msisdn, -1, "");
            context.write(new Text(mapKey), scoreWritable);
        }
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        JobExecuUtil execuUtil = new JobExecuUtil();
        URI[] localFiles = execuUtil.getCacheFiles(context);
        String recommAuthorClssOut = context.getConfiguration().get(
                PluginUtil.TEMP_AUTHOR_CLASSIFY_TABLE_KEY);
        recommAuthorClssOut = execuUtil.getFileName(recommAuthorClssOut);
        //格式：authorid|classid|count
        for (URI path : localFiles) {
            if (!path.toString().contains(recommAuthorClssOut)) {
                continue;
            }
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String authorid = fields[0];
                    String classid = fields[1];
                    List<String> authorClasses = authorClassMap.get(authorid);
                    if (authorClasses == null) {
                        authorClasses = new ArrayList<String>();
                    }
                    authorClasses.add(classid);
                    authorClassMap.put(authorid, authorClasses);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        String logStr = String.format("PrefAuthorFilterMapper set up"
                + " authorClassMap size %d", authorClassMap.size());
        System.out.println(logStr);
        //用户偏好
        //格式：msisdn|class1_id|score1|class2_id|score2|class3_id|score3
        for (URI path : localFiles) {
            if (!path.toString().contains(recommAuthorClssOut)) {
                continue;
            }
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String msisdn = fields[0];
                    List<String> prefClasses = new ArrayList<String>();
                    for (int i = 1; i < fields.length - 1; i += 2) {
                        String classid = fields[i];
                        String score = fields[i + 1];
                        if (classid.equalsIgnoreCase("null")
                                || classid.equalsIgnoreCase("\\N")
                                || score.equalsIgnoreCase("null")
                                || score.equalsIgnoreCase("\\N")) {
                            classid = null;
                        }
                        prefClasses.add(classid);
                    }
                    userPreferClassMap.put(msisdn, prefClasses);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        logStr = String.format("PrefAuthorFilterMapper set up"
                + " userPreferClassMap size %d", userPreferClassMap.size());
        System.out.println(logStr);
    }
}
