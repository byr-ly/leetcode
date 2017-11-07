package com.eb.bi.rs.andedu.theEntropyMethod;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/**standardize data
 * Created by linwanying on 2016/12/2.
 */
public class StandardizeMapper extends Mapper<Object, Text, Text, NullWritable> {
    private static Logger log;
    private int action_num;
    private String[] standardize_type;
    private double[] arrMax;
    private double[] arrMin;
    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        log = Logger.getLogger("StandardizeMapper");
        Configuration conf = context.getConfiguration();
        action_num = Integer.valueOf(conf.get("action_num"));
        arrMin = new double[action_num];
        arrMax = new double[action_num];
        standardize_type = String.valueOf(conf.get("standardize_type")).split("\\|");
        log.info("action_num:" + action_num);
        log.info("standardize_type:" + String.valueOf(conf.get("standardize_type")));
        URI[]  localFiles = context.getCacheFiles();
        if(localFiles == null){
            System.out.println("最大值最小值读取失败。");
            return;
        }
        for (URI path : localFiles) {
            String line;
            BufferedReader in = null;
            try {
                FileSystem fs = FileSystem.get(path, conf);
                in = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                while ((line = in.readLine()) != null) {
//                    log.info("line:" + line);
                    String[] val = line.split("\\s+");
//                    log.info("val.length:" + val.length);
                    if (val.length < 2) continue;
                    String[] values = val[1].split("\\|");
                    for (int i = 0; i < action_num; ++i) {
                        arrMax[i] = Double.parseDouble(values[i]);
                        arrMin[i] = Double.parseDouble(values[i+3]);
//                        log.info("arrMax[" + i + "]:" + arrMax[i]);
//                        log.info("arrMin[" + i + "]:" + arrMin[i]);
                    }
                }
            } finally {
                if (in != null) {
                    in.close();
                }
            }
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        String[] fields = value.toString().split("\\|");
        String keyOut = "";
        for (int i = 0; i < action_num; ++i) {
            if (arrMax[i] != arrMin[i]) {
                if (standardize_type[i].equals("1"))
                    fields[i+2] = String.valueOf((Double.valueOf(fields[i+2]) - arrMin[i])/(arrMax[i] - arrMin[i]));
                else
                    fields[i+2] = String.valueOf((arrMax[i] - Double.valueOf(fields[i+2]))/(arrMax[i] - arrMin[i]));
            } else {
                fields[i+2] = "1";
            }

        }

        for (int i = 0; i < fields.length - 1; ++i) {
            keyOut += fields[i] + "|";
        }
        keyOut += fields[action_num + 1];
        context.write(new Text(keyOut), NullWritable.get());
    }
}
