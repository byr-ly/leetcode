package com.eb.bi.rs.frame2.recframe.resultcal.offline.supplementer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

public class SupplementReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, String> fillterList = new HashMap<String, String>();//��ȷ��

    private int recommendMinum = 0;

    private String in_ifhaveSuffix;
    private String out_ifhaveSuffix;

    private String outputdataformatType;

    private String inputresultformatType;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //加载补白库文件
        recommendMinum = Integer.valueOf(context.getConfiguration().get("Appconf.result.recommend.minnum"));

        in_ifhaveSuffix = context.getConfiguration().get("Appconf.result.recommend.in.ifhaveSuffix");
        out_ifhaveSuffix = context.getConfiguration().get("Appconf.result.recommend.out.ifhaveSuffix");

        outputdataformatType = context.getConfiguration().get("Appconf.result.recommend.outputdataformatType");
        inputresultformatType = context.getConfiguration().get("Appconf.result.recommend.inputresultformatType");

        super.setup(context);
//        Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        URI[] localFiles = context.getCacheFiles();

        for (int i = 0; i < localFiles.length; i++) {
            String line;
            BufferedReader in = null;
            try {
                Path path = new Path(localFiles[i]);
                in = new BufferedReader(new FileReader(path.getName().toString()));
                while ((line = in.readLine()) != null) {
                    String fields[] = line.split("\\|");

                    if (fields.length != 1) {
                        fillterList.put(fields[0], line.substring(fields[0].length() + 1));
                    } else {
                        fillterList.put(fields[0], "");
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
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //待补白结果存储
        RecommendResult recommendResult = new RecommendResult();

        recommendResult.setmodel(inputresultformatType, outputdataformatType, in_ifhaveSuffix, out_ifhaveSuffix);

        //黑名单
        Set<String> blackList = new HashSet<String>();

        //输出结果
        List<String> result = new ArrayList<String>();

        //黑名单，推荐结果分离
        for (Text value : values) {
            String[] fields = value.toString().split("\\|");

            if (fields[0].equals("0")) {
                recommendResult.add(value.toString().substring(fields[0].length() + 1));
            } else {
                //黑名单
                blackList.add(fields[1]);
            }
        }

        //无需补白用户结果直接输出
        if (!recommendResult.iffiller(recommendMinum)) {
            result = recommendResult.getresult();
            for (int i = 0; i != result.size(); i++) {
                context.write(key, new Text(result.get(i)));
            }
            return;
        }

        //补白操作
        String itemKey = "";
        String itemVal = "";
        //补白输出循环
        Iterator<Entry<String, String>> it = fillterList.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, String> entry = (Entry<String, String>) it.next();
            itemKey = entry.getKey();
            itemVal = entry.getValue();

            if (!blackList.contains(itemKey)) {
                recommendResult.filler(itemKey, itemVal);
            }

            if (!recommendResult.iffiller(recommendMinum)) {
                result = recommendResult.getresult();
                for (int i = 0; i != result.size(); i++) {
                    context.write(key, new Text(result.get(i)));
                }
                break;
            }
        }
    }
}
