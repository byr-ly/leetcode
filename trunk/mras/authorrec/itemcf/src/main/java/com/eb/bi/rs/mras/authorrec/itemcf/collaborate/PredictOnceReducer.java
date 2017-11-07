package com.eb.bi.rs.mras.authorrec.itemcf.collaborate;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.PredictWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PredictOnceReducer
        extends Reducer<Text, PredictWritable, Text, DoubleWritable> {
    private Map<Integer, AuthorInfo> authorAvgScoreMap
            = new HashMap<Integer, AuthorInfo>();

    class AuthorInfo {
        String authorid;
        float score;

        public AuthorInfo(String id, float d) {
            this.authorid = id;
            this.score = d;
        }
    }

    /**
     * @param values: 格式：msisdn|authorI|author|score：其中msisdn对author有打分score
     *               reduce out: key:msisdn|authorI; value:predictScore
     */
    public void reduce(Text key, Iterable<PredictWritable> values,
                       Context context) throws IOException, InterruptedException {
        String[] keyStrs = key.toString().split("\\|");
        String msisdn = keyStrs[0];
        int authorReinx = Integer.parseInt(keyStrs[1]);
        float simSum = 0;
        float conSum = 0;
        AuthorInfo info = authorAvgScoreMap.get(authorReinx);
        if (info == null) {
            return;
        }
        for (PredictWritable value : values) {
            float sim = value.getSim();
            simSum += sim;
            conSum += value.getCon();
        }
        if (simSum == 0) {
            return;
        }
        float score = info.score + conSum / simSum;
        String outKey = String.format("%s|%s", msisdn, info.authorid);
        context.write(new Text(outKey), new DoubleWritable(score));
    }

    @Override
    public void setup(Reducer.Context context) throws IOException, InterruptedException {
        super.setup(context);
        JobExecuUtil jobExecuUtil = new JobExecuUtil();
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
                    String author = fields[1];
                    try {
                        int idx = Integer.parseInt(fields[0]);
                        float score = (float) Double.parseDouble(fields[2]);
                        AuthorInfo info = new AuthorInfo(author, score);
                        authorAvgScoreMap.put(idx, info);
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
        String strLog = String.format("setup authorAvgScoreMap "
                + "size: %d", authorAvgScoreMap.size());
        System.out.println(strLog);

    }
}
