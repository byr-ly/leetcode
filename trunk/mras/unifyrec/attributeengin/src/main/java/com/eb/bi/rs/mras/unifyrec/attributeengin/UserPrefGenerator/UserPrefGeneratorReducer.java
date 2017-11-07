package com.eb.bi.rs.mras.unifyrec.attributeengin.UserPrefGenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by LiMingji on 2015/10/16.
 */
public class UserPrefGeneratorReducer extends Reducer<Text, UserPrefVector, Text, Text> {
	private double new_values_avg = 0.0;
	private double serialize_values_avg = 0.0;
	private double famous_values_avg = 0.0;
	private double sales_values_avg = 0.0;
	private double pack_values_avg = 0.0;
	private double whol_values_avg = 0.0;
	private double chpt_values_avg = 0.0;
    private double man_values_avg = 0.0;
    private double female_values_avg = 0.0;
    private double low_values_avg = 0.0;
    private double high_values_avg = 0.0;
    private double hot_values_avg = 0.0;

    private HashMap<String, ArrayList<String>> simClass;

    @Override
	protected void reduce(Text key, Iterable<UserPrefVector> values, Context context)
			throws IOException, InterruptedException {
		Iterator<UserPrefVector> it = values.iterator();
		while (it.hasNext()) {
			UserPrefVector p = it.next();
            context.write(new Text(p.getWeight(new_values_avg, serialize_values_avg, famous_values_avg,
                    sales_values_avg, pack_values_avg, whol_values_avg, chpt_values_avg,
                    man_values_avg, female_values_avg, low_values_avg, high_values_avg,
                    hot_values_avg, simClass)), new Text(""));
        }
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
        simClass = new HashMap<String, ArrayList<String>>();

        System.out.printf("reduce setup");
        Configuration conf = context.getConfiguration();
        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

        if (localFiles == null) {
            System.out.println("没有找到均值File ");
        }
        for (int i = 0; i < localFiles.length; i++) {
            System.out.println("localFile: " + localFiles[i]);
            String line;
            BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));

            String fileName = localFiles[i].toString();
            if (fileName.contains("sim_classes")) {
                //分类 | 相似分类　｜相似度。
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|", -1);
                    if (fields.length < 3) {
                        continue;
                    }
                    String classId = fields[0];
                    String simClassId = fields[1];
                    String scores = fields[2];
                    if (simClass.containsKey(classId)) {
                        simClass.get(classId).add(simClassId + "|" + scores);
                    } else {
                        ArrayList<String> list = new ArrayList<String>();
                        list.add(simClassId + "|" + scores);
                        simClass.put(classId, list);
                    }
                }
                br.close();
                System.out.println("相似分类列表加载成功 " + simClass.size());
            } else {
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|", -1);
                    if (fields.length != 12) {
                        System.out.println("Bug!");
                        continue;
                    }
                    new_values_avg = getDouble(fields[0]);
                    serialize_values_avg = getDouble(fields[1]);
                    famous_values_avg = getDouble(fields[2]);
                    sales_values_avg = getDouble(fields[3]);
                    pack_values_avg = getDouble(fields[4]);
                    whol_values_avg = getDouble(fields[5]);
                    chpt_values_avg = getDouble(fields[6]);
                    man_values_avg = getDouble(fields[7]);
                    female_values_avg = getDouble(fields[8]);
                    low_values_avg = getDouble(fields[9]);
                    high_values_avg = getDouble(fields[10]);
                    hot_values_avg = getDouble(fields[11]);
                }
                br.close();
                System.out.println("当前均值为: " + new_values_avg + " " + serialize_values_avg + " " + famous_values_avg);
            }
        }

        /**
         * 获取每个分类的前两个相似分类。
         */
        Iterator<String> it = simClass.keySet().iterator();
        while (it.hasNext()) {
            String key = it.next();
            ArrayList<String> classes = simClass.get(key);
            Collections.sort(classes, simComp);
//System.out.println("现有分类:" + classes);
            ArrayList<String> topSimClasses = new ArrayList<String>();
            for (int i = 0; i < 5 && i < classes.size(); i++) {
                topSimClasses.add(classes.get(i).split("\\|", -1)[0]);
            }
//System.out.println("覆盖分类:" + topSimClasses);
            simClass.put(key, topSimClasses);
        }
    }

    private Comparator<String> simComp = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            double scores1 = getDouble(o1.split("\\|", -1)[1]);
            double scores2 = getDouble(o2.split("\\|", -1)[1]);
            if (scores1 > scores2) {
                return -1;
            }else if (scores2 > scores1) {
                return 1;
            }
            return 0;
        }
    };


    private Double getDouble(String input) {
        try {
            return Double.parseDouble(input.trim());
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}
