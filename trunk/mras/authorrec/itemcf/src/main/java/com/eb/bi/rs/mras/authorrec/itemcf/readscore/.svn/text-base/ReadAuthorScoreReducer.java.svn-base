package com.eb.bi.rs.mras.authorrec.itemcf.readscore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;


public class ReadAuthorScoreReducer
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private static double tolerable_error = 0.0000001;
    private Map<String, Integer> bookCountMap = new HashMap<String, Integer>();
    private Logger logger = null;
    /**
     * reduce out: key:msisdn|authorid; value:score
     */
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        int num = 0;
        int topNum = 0;
        int secNum = 0;
        double sum = 0;
        double maxScore = Double.MIN_VALUE;
        double minScore = Double.MAX_VALUE;
        String[] keyStrs = key.toString().split("\\|");
        String authorid = keyStrs[1];
        int auCnt = 0;
        Integer countValue = bookCountMap.get(authorid);
        if (countValue != null) {
            auCnt = countValue.intValue();
        }
        List<Double> scoreValues = new ArrayList<Double>();
        for (DoubleWritable value : values) {
            num++;
            double score = value.get();
            scoreValues.add(score);
            if (maxScore < score) {
                maxScore = score;
            }
            if (minScore > score) {
                minScore = score;
            }
            if (Math.abs(score - 5) < tolerable_error) {
                topNum++;
            }
            if (score >= 4 && score <= 5) {
                secNum++;
            }
            sum += score;
        }
        double avg = sum / num;
        //标准差
        double varSum = 0;
        for (double value : scoreValues) {
            double temp = value - avg;
            varSum += Math.pow(temp, 2);
        }
        double dev = Math.sqrt(varSum / num);
        double variation = dev / avg;
        double score = calcuScore(auCnt, num, topNum, secNum, minScore,
                maxScore, variation);
        context.write(key, new DoubleWritable(score));
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        logger = Logger.getLogger(ReadAuthorScoreReducer.class);
        JobExecuUtil execuUtil = new JobExecuUtil();
        String onshelfAuthorPath = context.getConfiguration().get(PluginUtil.TEMP_AUTHOR_CLASS_ONSHELF_COUNT_KEY);
        String authorBookCountOut = execuUtil.getFileName(onshelfAuthorPath);
        logger.info(authorBookCountOut);
        logger.info("ReadAuthorScoreReducer set up begin");
        URI[] uris = execuUtil.getCacheFiles(context);
        //格式：authorid|classid|bookNum
        for (URI path : uris) {
            if (!path.toString().contains(authorBookCountOut)) {
                continue;
            }
            System.out.println(path.toString());
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(path, context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String authorid = fields[0];
                    String countstr = fields[2];
                    int count = Integer.parseInt(countstr);
                    Integer authorBookCount = bookCountMap.get(authorid);
                    if (authorBookCount != null) {
                        count += authorBookCount.intValue();
                    }
                    bookCountMap.put(authorid, count);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        String logStr = String.format("ReadAuthorScoreReducer set up "
                + "bookCountMap size: %s", bookCountMap.size());
        System.out.println(logStr);
    }

    private double calcuScore(int auCnt, int num, int topNum, int secNum,
                              double min, double max, double variation) {
        double score = 0;
        double topRatio = 1.0 * topNum / num;
        double secRatio = 1.0 * secNum / num;
        if (num == 1) {
            if (Math.abs(max - 5) < tolerable_error) {
                if (auCnt == 1) {
                    score = max;
                } else if (auCnt > 1) {
                    score = max - 0.5;
                }
            } else if (max < 5) {
                score = max;
            }
        } else if (num == 2) {
            if (Math.abs(max - 5) < tolerable_error) {
                if (min >= 3.5) {
                    score = max;
                } else {
                    score = max - 0.5;
                }
            } else if (max < 5) {
                score = max;
            }
        } else if (num == 3 || num == 4) {
            if (topRatio >= 0.5) {
                score = max;
            } else {
                double power = Math.log(num) / Math.log(2); //log(2,n)
                if (variation >= 1) {
                    power /= num;
                }
                score = max - Math.pow(variation, power);
            }
        } else {
            if (topRatio >= 0.3 && secRatio >= 0.5) {
                score = max;
            } else if (topRatio >= 0.3 || secRatio >= 0.5) {
                double power = Math.log(num) / Math.log(2);
                if (variation >= 1) {
                    power /= num;
                }
                score = max - Math.pow(variation, power);
            }
        }
        if (score < 0) {
            score = 0;
        }
        return score;
    }

    @Override
    public void cleanup(Context context)
            throws IOException, InterruptedException {
        if (bookCountMap != null) {
            bookCountMap.clear();
            bookCountMap = null;
        }
    }

}
