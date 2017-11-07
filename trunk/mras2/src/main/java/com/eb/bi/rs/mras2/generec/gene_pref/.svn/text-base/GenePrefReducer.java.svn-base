package com.eb.bi.rs.mras2.generec.gene_pref;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by liyang on 2016/6/27.
 */
public class GenePrefReducer extends Reducer<Text, Text, Text, NullWritable> {

    private HashMap<String, ArrayList<String>> geneTypeMap;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text val : values) {
            ArrayList<String> similarTypeList = new ArrayList<String>();
            ArrayList<String> readTypeList = new ArrayList<String>();
            String[] line = val.toString().split("\\|");
            String firstTypePref = line[1];
            String secondTypePref = line[3];
            String thirdTypePref = line[5];
            String similarTypePref = line[7];
            String readTypePref = line[9];
            String class1 = line[0];
            String class2 = line[2];
            String class3 = line[4];
            String[] similarType = line[6].split(",");
            String[] readType = line[8].split(",");
            for (int i = 0; i < similarType.length; i++) {
                similarTypeList.add(similarType[i]);
            }
            for (int j = 0; j < readType.length; j++) {
                readTypeList.add(readType[j]);
            }

            double score = 0.0;
            Iterator iter = geneTypeMap.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                String gene = (String) entry.getKey();
                ArrayList<String> typeList = geneTypeMap.get(gene);
                ArrayList<String> typeListCopyOne = new ArrayList<String>();
                ArrayList<String> typeListCopyTwo = new ArrayList<String>();

                if(!typeList.isEmpty()) typeListCopyOne.addAll(typeList);
                if(!typeList.isEmpty()) typeListCopyTwo.addAll(typeList);
                if (typeList.contains(class1)) score += Double.parseDouble(firstTypePref);
                if (typeList.contains(class2)) score += Double.parseDouble(secondTypePref);
                if (typeList.contains(class3)) score += Double.parseDouble(thirdTypePref);
                typeListCopyOne.retainAll(similarTypeList);
                if (typeListCopyOne.size() != 0) score += Double.parseDouble(similarTypePref);
                typeListCopyTwo.retainAll(readTypeList);
                if (typeListCopyTwo.size() != 0) score += Double.parseDouble(readTypePref);
                context.write(new Text(key.toString() + "|" + gene + "|" + score), NullWritable.get());
                score = 0.0;
            }
        }
    }

    protected void setup(Context context) throws IOException, InterruptedException {

        geneTypeMap = new HashMap<String, ArrayList<String>>();

        System.out.printf("reduce setup");
        Configuration conf = context.getConfiguration();
        //Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

        URI[] localFiles = context.getCacheFiles();
        for (int i = 0; i < localFiles.length; ++i) {
            String line;
            BufferedReader in = null;
            try {
                /*DistributedCache修改点*/
                FileSystem fs = FileSystem.get(localFiles[i], conf);
                in = new BufferedReader(new InputStreamReader(fs.open(new Path(localFiles[i]))));

                if (localFiles[i].toString().contains("00")) {
                    while ((line = in.readLine()) != null) {//基因 | 名称 | 分类
                        String[] fields = line.split("\\|");
                        if (fields.length < 3) {
                            continue;
                        }
                        String gene = fields[0];
                        String[] type = fields[2].split(",");
                        if (!geneTypeMap.containsKey(gene)) {
                            ArrayList<String> typeList = new ArrayList<String>();
                            for (int j = 0; j < type.length; j++) {
                                if (typeList.contains(type[j])) continue;
                                typeList.add(type[j]);
                            }
                            geneTypeMap.put(gene, typeList);
                        } else {
                            ArrayList<String> typeList = geneTypeMap.get(gene);
                            for (int j = 0; j < type.length; j++) {
                                if (typeList.contains(type[j])) continue;
                                typeList.add(type[j]);
                            }
                            geneTypeMap.put(gene, typeList);
                        }
                    }
                }
            } finally {
                if (in != null) {
                    in.close();
                }
            }
        }
    }
}

