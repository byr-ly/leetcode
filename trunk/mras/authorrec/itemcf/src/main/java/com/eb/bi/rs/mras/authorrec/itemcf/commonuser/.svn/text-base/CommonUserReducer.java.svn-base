package com.eb.bi.rs.mras.authorrec.itemcf.commonuser;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by liyang on 2016/3/8.
 */
public class CommonUserReducer extends Reducer<Text, Text, Text, NullWritable> {
    Logger log = Logger.getLogger(CommonUserReducer.class);

    private HashMap<String, ArrayList<String>> commonUser;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        commonUser = new HashMap<String, ArrayList<String>>();
        for (Text val : values) {
            String[] line = val.toString().split("\\|");
            if (line.length >= 3) {
                String writer = line[1];
                if (commonUser.containsKey(writer)) {
                    commonUser.get(writer).add(line[0]);
                } else {
                    ArrayList<String> userList = new ArrayList<String>();
                    userList.add(line[0]);
                    commonUser.put(writer, userList);
                }
            }
        }

        Iterator<Map.Entry<String, ArrayList<String>>> it = commonUser.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ArrayList<String>> entry = it.next();
            String writer = entry.getKey();
            if (commonUser.get(writer).size() <= 10) {
                it.remove();
            }
        }

        //遍历作家|用户列表，找到作家与作家之间的共同用户
        Iterator outIter = commonUser.entrySet().iterator();
        while (outIter.hasNext()) {
            Map.Entry outEntry = (Map.Entry) outIter.next();
            String outWriter = (String) outEntry.getKey();
            ArrayList<String> outUserList = commonUser.get(outWriter);
            Iterator inIter = commonUser.entrySet().iterator();
            while (inIter.hasNext()) {
                ArrayList<String> outUserCopyList = new ArrayList<String>();
                outUserCopyList.addAll(outUserList);
                Map.Entry inEntry = (Map.Entry) inIter.next();
                String inWriter = (String) inEntry.getKey();
                if (outWriter.compareTo(inWriter) == 0) continue;
                else {
                    ArrayList<String> inUserList = commonUser.get(inWriter);
                    outUserCopyList.retainAll(inUserList);
                    if (outUserCopyList.size() <= 10) {
                        continue;
                    } else {
                        StringBuffer users = new StringBuffer();
                        for (int i = 0; i < outUserCopyList.size(); i++) {
                            users.append(outUserCopyList.get(i) + "|");
                        }
                        users.deleteCharAt(users.length() - 1);
                        context.write(new Text(outWriter + "|" + inWriter + "|" + users), NullWritable.get());
                    }
                }
            }
        }
    }
}
