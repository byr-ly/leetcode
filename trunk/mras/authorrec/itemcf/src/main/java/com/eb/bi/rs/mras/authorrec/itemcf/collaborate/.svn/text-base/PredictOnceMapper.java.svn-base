package com.eb.bi.rs.mras.authorrec.itemcf.collaborate;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.PredictWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PredictOnceMapper extends Mapper<Object, Text, Text, PredictWritable> {
    private Map<Integer, Float> authorAvgScoreMap = new HashMap<Integer, Float>();
    private Map<Integer, Map<String, Float>> userScoreMap
            = new HashMap<Integer, Map<String, Float>>();
    private float authorSimMin = 0.0f;


    /**
     * @param value: 格式：格式：msisdn|authorid|score
     *               map out:
     *               key: ; value:
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        //	String authorid1 = strs[0];
        //	String authorid2 = strs[1];
        int authorid1 = Integer.parseInt(strs[0]);
        int authorid2 = Integer.parseInt(strs[1]);
        float simScore = Float.parseFloat(strs[2]);
        if (simScore < authorSimMin) {
            return;
        }
        Map<String, Float> predict1 = getPredictUsers(authorid1, authorid2);
        for (Entry<String, Float> entry : predict1.entrySet()) {
            String mapKey = String.format("%s|%d", entry.getKey(), authorid2);
            float con = simScore * (entry.getValue() - authorAvgScoreMap.get(authorid1));
            PredictWritable writable = new PredictWritable(simScore, con);
            context.write(new Text(mapKey), writable);
        }
        Map<String, Float> predict2 = getPredictUsers(authorid2, authorid1);
        for (Entry<String, Float> entry : predict2.entrySet()) {
            String mapKey = String.format("%s|%d", entry.getKey(), authorid1);
            float con = simScore * (entry.getValue() - authorAvgScoreMap.get(authorid2));
            PredictWritable writable = new PredictWritable(simScore, con);
            context.write(new Text(mapKey), writable);
        }
    }

    private Map<String, Float> getPredictUsers(int joinId, int recommId) {
        Map<String, Float> predictUsers = new HashMap<String, Float>();
        Map<String, Float> joinMap = userScoreMap.get(joinId);
        Map<String, Float> tempMap = userScoreMap.get(recommId);
        if (joinMap == null) {
            return predictUsers;
        }
        for (Entry<String, Float> entry : joinMap.entrySet()) {
            String msisdn = entry.getKey();
            if (tempMap != null && tempMap.containsKey(msisdn)) {
                continue;
            }
            predictUsers.put(msisdn, entry.getValue());
        }
        return predictUsers;
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        JobExecuUtil jobExecuUtil = new JobExecuUtil();
        authorSimMin = context.getConfiguration().getFloat(
                PluginUtil.COLL_AUTHOR_SIM_MIN, authorSimMin);
        URI[] localFiles = jobExecuUtil.getCacheFiles(context);
        String authorAvgScorePath = context.getConfiguration()
                .get(PluginUtil.TEMP_COLL_AUTHOR_REINDEX_KEY);
        authorAvgScorePath = jobExecuUtil.getFileName(authorAvgScorePath);
        System.out.println(authorAvgScorePath);
        //格式：idx|author|avg
        for (URI path : localFiles) {
            if (!path.toString().contains(authorAvgScorePath)) {
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
                    try {
                        //		String id = fields[0];
                        int id = Integer.parseInt(fields[0]);
                        float score = (float) Double.parseDouble(fields[2]);
                        authorAvgScoreMap.put(id, score);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        String strLog = String.format("PredictReducer setup authorAvgScoreMap "
                + "size: %d", authorAvgScoreMap.size());
        System.out.println(strLog);
    /*	String userScorePath = context.getConfiguration()
                .get(PluginUtil.TEMP_AUTHOR_REINDEX_SCORE_FILTER_OUT);
		userScorePath = jobExecuUtil.getFileName(userScorePath);
		System.out.println(userScorePath);*/
        //格式：user|authorid|score
        for (URI path : localFiles) {
            if (!path.toString().contains(authorAvgScorePath)) {
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
                    int authorid = Integer.parseInt(fields[1]);
                    //			String authorid = fields[1];
                    float score = (float) Double.parseDouble(fields[2]);
                    Map<String, Float> scoreMap = userScoreMap.get(authorid);
                    if (scoreMap == null) {
                        scoreMap = new HashMap<String, Float>();
                    }
                    scoreMap.put(msisdn, score);
                    userScoreMap.put(authorid, scoreMap);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        strLog = String.format("setup userScoreMap "
                + "size: %d", userScoreMap.size());
        System.out.println(strLog);
    }
}
