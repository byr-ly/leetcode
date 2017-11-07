package com.eb.bi.rs.mras2.unifyrec.greylist;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class WriteGreyListReducer extends
		Reducer<Text, NullWritable, Text, NullWritable> {

	private MultipleOutputs<Text, NullWritable> mos;

	private String split = null;
	private int size = 0;
	private SimpleDateFormat sdf = null;
	private String blackListPath = null;
	private String greyListPath = null;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// 初始化多输出
		mos = new MultipleOutputs<Text, NullWritable>(context);

		Configuration conf = context.getConfiguration();
		split = conf.get("conf.import.split");
		size = Integer.parseInt(conf.get("conf.data.size"));
		blackListPath = conf.get("conf.blacklist.output.path");
		greyListPath = conf.get("conf.greylist.output.path");
		sdf = new SimpleDateFormat("yyyy-MM-dd");
	}

	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {
		String[] datas = key.toString().split(split, -1);
		if (datas.length == size) {
			String userid = datas[0];
			String bookid = datas[1];
			int count = Integer.parseInt(datas[2]);
			if(userid != null && !"".equals(userid) && bookid != null && !"".equals(bookid)){
				String rowKey = userid + "_" + bookid;
				String date = "";
				Calendar calendar = Calendar.getInstance();
				if (count == 3) {
					calendar.add(Calendar.DAY_OF_YEAR, +15);
					date = sdf.format(calendar.getTime());
					mos.write(new Text(rowKey + "|" + date), NullWritable.get(),
							greyListPath);
				} else if (count == 6) {
					calendar.add(Calendar.DAY_OF_YEAR, +30);
					date = sdf.format(calendar.getTime());
					mos.write(new Text(rowKey + "|" + date), NullWritable.get(),
							greyListPath);
				} else if (count == 9) {
					calendar.add(Calendar.DAY_OF_YEAR, +45);
					date = sdf.format(calendar.getTime());
					mos.write(new Text(rowKey + "|" + date), NullWritable.get(),
							greyListPath);
				} else if (count >= 12) {
					mos.write(new Text(userid + "_" + bookid + "|1"),	//1表示type=1，灰名单生成的黑名单数据
							NullWritable.get(),  blackListPath);
				}
			}
			
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

}
