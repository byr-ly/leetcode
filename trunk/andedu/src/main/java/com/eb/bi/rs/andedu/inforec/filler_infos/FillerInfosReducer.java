package com.eb.bi.rs.andedu.inforec.filler_infos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;



public class FillerInfosReducer extends Reducer<Text, Text, Text, Text> {
	private static int fillerTopN = 100;
	private static int resultNum = 100;
	private static int eachResultNum = 10;

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("begin reduce");
		int num = 0;
		HashMap<String, Double> newsMap = new HashMap<String, Double>();
		ValueComparator bvc = new ValueComparator(newsMap);
		TreeMap<String, Double> sorted_map = new TreeMap<String, Double>(bvc);
		for (Text text : values) {
			String[] field = text.toString().split("\\|");
			newsMap.put(field[0], Double.parseDouble(field[1]));
		}
		sorted_map.putAll(newsMap);
		StringBuffer sb = new StringBuffer("");
		Set<Entry<String, Double>> entrySet = sorted_map.entrySet();
		for (Entry<String, Double> entry : entrySet) {
			if (num >= fillerTopN) {
				break;
			}
			num++;
			sb.append(entry.getKey());
			sb.append("|");
		}
		System.out.println("++++: " + sb.toString());
		Log.info("++++: " + sb.toString());
		context.write(new Text("info_filler"), new Text(sb.toString()));
		Random r = new Random();
		for (int i = 0; i < resultNum; i++) {
			StringBuffer sbr = new StringBuffer("");
			List<Entry<String, Double>> list = new ArrayList<Entry<String, Double>>();
			list.addAll(entrySet);
			Set<String> set = new HashSet<String>();
			for (int j = 0 ; set.size() < eachResultNum && set.size() < entrySet.size(); j++) {
				Entry<String, Double> entry = list.get(r.nextInt(num));
				set.add(entry.getKey());
			}
			for (String string : set) {
				sbr.append(string);
				sbr.append("|");
			}
			System.out.println("++++: " + sbr.toString());
			Log.info("++++: " + sbr.toString());
			context.write(new Text(String.valueOf(i)), new Text(sbr.toString()));
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
      //Logger log = PluginUtil.getInstance().getLogger();
      Logger log = Logger.getLogger(FillerInfosReducer.class);
      try {
      	fillerTopN = Integer.parseInt(context.getConfiguration().get("fillerTopN"));
      } catch (NumberFormatException e) {
      	fillerTopN = 100;
      }
      try {
      	resultNum = Integer.parseInt(context.getConfiguration().get("resultNum"));
      } catch (NumberFormatException e) {
      	resultNum = 100;
      }
      try {
      	eachResultNum = Integer.parseInt(context.getConfiguration().get("eachResultNum"));
      } catch (NumberFormatException e) {
      	eachResultNum = 10;
      }

	}
}


class ValueComparator implements Comparator<String> {

	Map<String, Double> base;

	public ValueComparator(Map<String, Double> base) {
		this.base = base;
	}

	// Note: this comparator imposes orderings that are inconsistent with
	// equals.
	public int compare(String a, String b) {
		if (base.get(a) >= base.get(b)) {
			return -1;
		} else {
			return 1;
		} // returning 0 would merge keys
	}

}
