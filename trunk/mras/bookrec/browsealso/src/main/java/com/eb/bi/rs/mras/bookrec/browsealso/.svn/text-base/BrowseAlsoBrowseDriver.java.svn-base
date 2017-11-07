package com.eb.bi.rs.mras.bookrec.browsealso;

import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.net.URI;
import java.util.*;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.apache.log4j.Logger;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
import com.eb.bi.rs.frame.recframe.base.BaseDriver;
import com.eb.bi.rs.frame.recframe.base.JobComponent;
import com.eb.bi.rs.frame.recframe.base.ComponentHelper;


public class BrowseAlsoBrowseDriver extends BaseDriver {

	private static final Logger LOG = Logger.getLogger(BrowseAlsoBrowseDriver.class);

	public static void main(String[] args) throws Exception {
		PluginUtil pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		Logger log = pluginUtil.getLogger();

		PluginConfig pluginConfig = pluginUtil.getConfig();
		JobComponent root = ComponentHelper.createComposite(pluginConfig.getElement("composite"));

		Date begin = new Date();

		int ret = root.run(null);

		Date end = new Date();
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
		String endTime = format.format(end);
		long timeCost = end.getTime() - begin.getTime();

		PluginResult result = pluginUtil.getResult();
		result.setParam("endTime", endTime);
		result.setParam("timeCosts", timeCost);
		result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
		result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
		result.save();

		log.info("time cost in total(s): " + (timeCost / 1000.0));
		System.exit(ret);
	}


	@Override
	public int run(String[] args) throws Exception {
		String numStr = properties.getProperty("conf.num.reduce.tasks", "1");
		int numReducers = Integer.parseInt(numStr);


		// 数据准备
		Configuration conf = new Configuration(getConf());

		Job job = new Job(conf, "browsealso-prepare");
		job.setJarByClass(PrepareMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(PrepareMapper.class);
		job.setReducerClass(PrepareReducer.class);
		job.setNumReduceTasks(numReducers);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		String inputPath = properties.getProperty("conf.input.path");
		FileInputFormat.setInputPaths(job, inputPath);
		String prepareOutputPath = "browsealso/prepare";
		SequenceFileOutputFormat.setOutputPath(job, new Path(prepareOutputPath));
		check(prepareOutputPath);

		LOG.info("job " + job.getJobName() + "...");
		if (!job.waitForCompletion(true)) {
			return 1;
		}

		// 计算图书的用户数
		conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator", "|");
		conf.set("conf.max.book.per.user", properties.getProperty("conf.max.book.per.user", "300"));
		job = new Job(conf, "browsealso-sum(userNum)");
		job.setJarByClass(UserNumSumReducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setReducerClass(UserNumSumReducer.class);
		job.setNumReduceTasks(numReducers);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		SequenceFileInputFormat.setInputPaths(job, prepareOutputPath);
		MultipleOutputs.addNamedOutput(job, UserNumSumReducer.BOOK, TextOutputFormat.class, Text.class, LongWritable.class);
		MultipleOutputs.addNamedOutput(job, UserNumSumReducer.COOCCURRENCE, SequenceFileOutputFormat.class, Text.class, LongWritable.class);
		String sumOutput = "browsealso/sum";
		FileOutputFormat.setOutputPath(job, new Path(sumOutput));
		check(sumOutput);

		LOG.info("job " + job.getJobName() + "...");
		if (!job.waitForCompletion(true)) {
			return 1;
		}

		// 计算置信度等指标
		conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator", "|");
		conf.set("conf.min.book.num", properties.getProperty("conf.min.book.num", "100"));
		conf.set("conf.min.cooccur.num", properties.getProperty("conf.min.cooccur.num", "5"));
		conf.set("conf.kulc.threshold", properties.getProperty("conf.kulc.threshold", "0.05"));
		conf.set("conf.ir.threshold", properties.getProperty("conf.ir.threshold", "0.7"));
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.globStatus(new Path(sumOutput, UserNumSumReducer.BOOK_PATH + "*"));
		for (FileStatus st : status) {
			DistributedCache.addCacheFile(URI.create(st.getPath().toString()), conf);
		}

		job = new Job(conf, "browsealso-computeindex");
		job.setJarByClass(IndexMapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(IndexMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(sumOutput, UserNumSumReducer.COOCCURRENCE_PATH + "*"));
		String midOutputPath = properties.getProperty("conf.mid.output.path");
		FileOutputFormat.setOutputPath(job, new Path(midOutputPath));
		check(midOutputPath);

		LOG.info("job " + job.getJobName() + "...");
		if (!job.waitForCompletion(true)) {
			return 1;
		}

		// 计算图书频度
		conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator", "|");
		String bookUV = properties.getProperty("conf.book.uv.path");
		fs = FileSystem.get(conf);
		status = fs.globStatus(new Path(bookUV));
		for (FileStatus st : status) {
			DistributedCache.addCacheFile(URI.create(st.getPath().toString()), conf);
		}

		job = new Job(conf, "browsealso-computefreq");
		job.setJarByClass(FrequencyMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(FrequencyMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		String bookClassPath = properties.getProperty("conf.book.class.path");
		FileInputFormat.setInputPaths(job, bookClassPath);
		String freqOutputPath = "browsealso/bookfreq";
		FileOutputFormat.setOutputPath(job, new Path(freqOutputPath));
		check(freqOutputPath);

		LOG.info("job " + job.getJobName() + "...");
		if (!job.waitForCompletion(true)) {
			return 1;
		}


		// 推荐结果及结果补白
		conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator", "|");
		conf.set("conf.top.n", properties.getProperty("conf.top.n", "10"));
		status = fs.globStatus(new Path(freqOutputPath + "/part*"));
		for (FileStatus st : status) {
			DistributedCache.addCacheFile(URI.create(st.getPath().toString()), conf);
		}

		job = new Job(conf, "browsealso-supplement");
		job.setJarByClass(SupplementMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(SupplementMapper.class);
		job.setReducerClass(SupplementReducer.class);
		job.setNumReduceTasks(numReducers);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, midOutputPath);
		String partialOutputPath = "browsealso/partialoutput";
		FileOutputFormat.setOutputPath(job, new Path(partialOutputPath));
		check(partialOutputPath);

		LOG.info("job " + job.getJobName() + "...");
		if (!job.waitForCompletion(true)) {
			return 1;
		}

		// 纯补白
		conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator", "|");
		conf.set("conf.top.n", properties.getProperty("conf.top.n", "10"));
		status = fs.globStatus(new Path(freqOutputPath + "/part*"));
		for (FileStatus st : status) {
			DistributedCache.addCacheFile(URI.create(st.getPath().toString()), conf);
		}

		job = new Job(conf, "browsealso-puresupplement");
		job.setJarByClass(PureSupplementMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(PureSupplementMapper.class);
		job.setReducerClass(PureSupplementReducer.class);
		job.setNumReduceTasks(numReducers);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(sumOutput, UserNumSumReducer.BOOK_PATH + "*"), new Path(partialOutputPath));
		String outputPath = properties.getProperty("conf.output.path");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);

		LOG.info("job " + job.getJobName() + "...");
		if (!job.waitForCompletion(true)) {
			return 1;
		}


		return 0;
	}

	public void check(String fileName) {
		try {
			FileSystem fs = FileSystem.get(URI.create(fileName), new Configuration());
			Path f = new Path(fileName);
			if (fs.exists(f)) {
				fs.delete(f, true);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 将用户ID和图书ID转换为Long型
	 */
	public static class PrepareMapper extends Mapper<Object, Text, LongWritable, LongWritable> {

		/**
		 * @param value userId|bookId
		 */
		@Override
		protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
			String[] fields = value.toString().split("\\|");
			if (fields.length != 2) {
				return;
			}

			long userId = Long.parseLong(fields[0]);
			long bookId = Long.parseLong(fields[1]);
			ctx.write(new LongWritable(userId), new LongWritable(bookId));
		}
	}

	/**
	 * 去掉阅读图书超过限定数量的用户数据
	 */
	public static class PrepareReducer extends Reducer<LongWritable, LongWritable, Text, LongWritable> {

		private int maxBookPerUser = 300;
		private static final LongWritable ONE = new LongWritable(1L);
		private List<Long> bookList = new ArrayList<Long>();

		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			super.setup(ctx);

			maxBookPerUser = ctx.getConfiguration().getInt("conf.max.book.per.user", 300);
		}

		@Override
		protected void reduce(LongWritable userId, Iterable<LongWritable> bookIds, Context ctx) throws IOException, InterruptedException {
			bookList.clear();
			int cnt = 0;
			for (LongWritable bookId : bookIds) {
				cnt++;
				if (cnt > maxBookPerUser) {
					break;
				}
				bookList.add(bookId.get());
			}

			if (cnt > maxBookPerUser) {
				return;
			}

			Collections.sort(bookList);

			for (int i = 0; i < cnt; i++) {
				ctx.write(new Text(bookList.get(i).toString()), ONE);
				for (int j = i + 1; j < cnt; j++) {
					ctx.write(new Text(bookList.get(i) + "|" + bookList.get(j)), ONE);
				}
			}
		}
	}

	/**
	 * 对用户数求和
	 */
	public static class UserNumSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		public static final String BOOK = "book";
		public static final String BOOK_PATH = "book/part";
		public static final String COOCCURRENCE = "cooccurrence";
		public static final String COOCCURRENCE_PATH = "cooccurrence/part";

		private MultipleOutputs<Text, LongWritable> mos;

		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, LongWritable>(ctx);
		}

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context ctx) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}

			if (-1 == key.toString().indexOf('|')) {
				mos.write(BOOK, key, new LongWritable(sum), BOOK_PATH);
			} else {
				mos.write(COOCCURRENCE, key, new LongWritable(sum), COOCCURRENCE_PATH);
			}
		}

		@Override
		protected void cleanup(Context ctx) throws IOException, InterruptedException {
			mos.close();
		}
	}

	/**
	 * 计算相关指标
	 */
	public static class IndexMapper extends Mapper<Text, LongWritable, LongWritable, Text> {

		public int bookNumMin = 100;
		public int cooccurNumMin = 5;
		public double kulcThreshold = 0.05;
		public double irThreshold = 0.7;
			
		private Map<Long, Long> books = new HashMap<Long, Long>();

		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			super.setup(ctx);

			Configuration conf = ctx.getConfiguration();
			bookNumMin = conf.getInt("conf.min.book.num", 100);
			cooccurNumMin = conf.getInt("conf.min.cooccur.num", 5);
			kulcThreshold = conf.getFloat("conf.kulc.threshold", 0.05f);
			irThreshold = conf.getFloat("conf.ir.threshold", 0.7f);

			Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
			for (Path path : localFiles) {
				BufferedReader br = null;
				try {
					String file = path.toString();
					br = new BufferedReader(new FileReader(file));
					String line;
					String[] fields = null;
					while ((line = br.readLine()) != null) {
						fields = line.split("\\|");
						if (fields.length < 2) {
							continue;
						}

						long bookId = Long.parseLong(fields[0]);
						long userNum = Long.parseLong(fields[1]);
						books.put(bookId, userNum);
					}
				} finally {
					if (br != null) {
						br.close();
					}
				}
			}   
		}

		@Override
		protected void map(Text key, LongWritable value, Context ctx) throws IOException, InterruptedException {
			String[] fields = key.toString().split("\\|");
			long numAB = value.get();
			if (numAB < cooccurNumMin || fields.length < 2) {
				return;
			}

			long bookA = Long.parseLong(fields[0]);
			long bookB = Long.parseLong(fields[1]);
			long numA = books.get(bookA);
			long numB = books.get(bookB);

			if (numA <= bookNumMin && numB <= bookNumMin) {
				return;
			}

			double pAB = (double) numAB / (double) numA;
			double pBA = (double) numAB / (double) numB;
			double kulc = (pAB + pBA) / 2.0;

			if (numB > bookNumMin) {
				double ir = pBA / pAB;
				int classType = 2;
				if (kulc > kulcThreshold && ir > irThreshold) {
					classType = 1;
				}


				String valueOut = String.format("%s|%s|%s|%s|%s|%s|%s|%s|%s",
												bookB, numA, numB, numAB, pAB, pBA, kulc, ir, classType);
				ctx.write(new LongWritable(bookA), new Text(valueOut));

			}

			if (numA > bookNumMin) {
				double ir = pAB / pBA;
				int classType = 2;
				if (kulc > kulcThreshold && ir > irThreshold) {
					classType = 1;
				}

				String valueOut = String.format("%s|%s|%s|%s|%s|%s|%s|%s|%s",
												bookA, numB, numA, numAB, pBA, pAB, kulc, ir, classType);
				ctx.write(new LongWritable(bookB), new Text(valueOut));

			}
		}
	}

	public static class FrequencyMapper extends Mapper<Object, Text, Text, LongWritable> {

		private Map<Long, Long> freqMap = new HashMap<Long, Long>();

		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			super.setup(ctx);

			Configuration conf = ctx.getConfiguration();

			Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
			for (Path path : localFiles) {
				BufferedReader br = null;
				try {
					String file = path.toString();
					br = new BufferedReader(new FileReader(file));
					String line;
					String[] fields = null;
					while ((line = br.readLine()) != null) {
						fields = line.split("\\|");
						if (fields.length < 2) {
							continue;
						}

						long bookId = Long.parseLong(fields[0]);
						long freq = Long.parseLong(fields[1]);
						freqMap.put(bookId, freq);
					}
				} finally {
					if (br != null) {
						br.close();
					}
				}
			} 
		}

		@Override
		protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
			String[] fields = value.toString().split("\\|");
			if (fields.length < 2) {
				return;
			}

			long bookId = Long.parseLong(fields[0]);
			if (freqMap.containsKey(bookId)) {
				ctx.write(value, new LongWritable(freqMap.get(bookId)));
			}
		}
	}

	/**
	 * 
	 */
	public static class SupplementMapper extends Mapper<Object, Text, LongWritable, Text> {

		/**
		 * @param value bookA|bookB|numA|numB|numAB|pAB|pBA|kulc|ir|classtype
		 */
		@Override
		protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
			String[] fields = value.toString().split("\\|");
			long bookA = Long.parseLong(fields[0]);
			String valueOut = fields[1] + "|" + fields[5] + "|" + fields[9];

			ctx.write(new LongWritable(bookA), new Text(valueOut));
		}
	}

	/**
	 *
	 */
	public static class SupplementReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

		private int topN;
		private double firstMin;
		private double firstMax;
		private Map<String, TreeSet<LongPair>> classMap = new HashMap<String, TreeSet<LongPair>>();
		private Map<Long, String> bookMap = new HashMap<Long, String>();

		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			super.setup(ctx);

			Configuration conf = ctx.getConfiguration();
			topN = conf.getInt("conf.top.n", 10);
			firstMin = conf.getFloat("conf.first.min", 0.15f);
			firstMax = conf.getFloat("conf.first.max", 0.65f);

			Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
			for (Path path : localFiles) {
				BufferedReader br = null;
				try {
					String file = path.toString();
					br = new BufferedReader(new FileReader(file));
					String line;
					String[] fields = null;
					while ((line = br.readLine()) != null) {
						fields = line.split("\\|");
						if (fields.length < 3) {
							continue;
						}

						long bookId = Long.parseLong(fields[0]);
						long freq = Long.parseLong(fields[2]);
						String classType = fields[1];
						bookMap.put(bookId, classType);
						if (classMap.containsKey(classType)) {
							classMap.get(classType).add(new LongPair(freq, bookId));
						} else {
							TreeSet<LongPair> ts = new TreeSet<LongPair>();
							ts.add(new LongPair(freq, bookId));
							classMap.put(classType, ts);
						}
					}
				} finally {
					if (br != null) {
						br.close();
					}
				}
			}   
		}

		@Override
		protected void reduce(LongWritable bookA, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
			TreeSet<String> bookBs = new TreeSet<String>(new Comparator<String>() {
					@Override
					public int compare(String s1, String s2) {
						String[] fields1 = s1.split("\\|");
						String[] fields2 = s2.split("\\|");
						if (fields1[2].compareTo(fields2[2]) != 0) {
							return fields1[2].compareTo(fields2[2]);
						} else if (fields1[1].compareTo(fields2[1]) != 0) {
							return fields2[1].compareTo(fields1[1]);
						} else {
							return fields1[0].compareTo(fields2[0]);
						}
					}
				});

			for (Text value : values) {
				bookBs.add(value.toString());
				if (bookBs.size() > topN) {
					bookBs.remove(bookBs.last());
				}
			}

			TreeSet<Long> bs = new TreeSet<Long>();
			bs.add(bookA.get());
			Random rand = new Random();
			Iterator<String> iter = bookBs.iterator();
			int seqNum = 1;
			String[] fields = iter.next().split("\\|");
			StringBuffer valueOut = new StringBuffer();
			double pAB = Double.parseDouble(fields[1]);
			String bookId = fields[0];
			if (pAB < firstMin || pAB > firstMax) {
				pAB = rand.nextDouble() * (firstMax - firstMin) + firstMin;
			}
			valueOut.append(String.format("%s|%.1f%%", bookId, 100.0 * pAB));
			bs.add(Long.parseLong(bookId));

			double restMinRate;
			if (pAB > 0.45) {
				restMinRate = 0.5;
			} else if (pAB > 0.3) {
				restMinRate = 0.65;
			} else {
				restMinRate = 0.75;
			}
			while (iter.hasNext()) {
				seqNum++;
				fields = iter.next().split("\\|");
				bookId = fields[0];
				double restMin = restMinRate * pAB;
				if (restMin < 0.05) {
					restMin = 0.05;
				}
				pAB = rand.nextDouble() * (pAB - restMin) + restMin;
				valueOut.append(String.format("|%s|%.1f%%", bookId, 100.0 * pAB));
				bs.add(Long.parseLong(bookId));
			}

			if (seqNum < topN) {

				String ct = bookMap.get(bookA.get());
				if (null == ct) {
					return;
				}
				TreeSet<LongPair> sps = classMap.get(ct);
				if (null == sps) {
					return ;
				}
				
				for (LongPair sp : sps) {
					Long bookB = sp.getSecond();
					if (bs.contains(bookB)) {
						continue;
					}
					seqNum++;
					double restMin = restMinRate * pAB;
					if (restMin < 0.05) {
						restMin = 0.05;
					}
					pAB = rand.nextDouble() * (pAB - restMin) + restMin;
					valueOut.append(String.format("|%s|%.1f%%", bookB, 100.0 * pAB));

					if (seqNum >= topN) {
						break;
					}
				}
			}
			ctx.write(bookA, new Text(valueOut.toString()));
		}
	}

	public static class LongPair implements Comparable<LongPair> {
		private Long first;
		private Long second;

		public LongPair(long first, long second) {
			this.first = first;
			this.second = second;
		}

		public Long getFirst() {
			return first;
		}

		public Long getSecond() {
			return second;
		}

		@Override
		public int compareTo(LongPair that) {
			if (first.compareTo(that.first) != 0) {
				return that.first.compareTo(first);
			} else {
				return second.compareTo(that.second);
			}
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof LongPair)) {
				return false;
			}

			LongPair that = (LongPair) o;
			return first.equals(that.first) && second.equals(that.second);
		}

		@Override
		public int hashCode() {
			return (first.hashCode() + 41) * 41 + second.hashCode();
		}
	}

	public static class PureSupplementMapper extends Mapper<Object, Text, LongWritable, Text> {

		@Override
		protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
			String[] fields = value.toString().split("\\|", 2);
			if (fields.length != 2) {
				return;
			}

			long bookId = Long.parseLong(fields[0]);
			if (fields[1].indexOf('|') != -1) {
				ctx.write(new LongWritable(bookId), new Text(fields[1]));
			} else {
				ctx.write(new LongWritable(bookId), new Text(""));
			}
		}
	}

	public static class PureSupplementReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

		private int topN;
		private Map<String, TreeSet<LongPair>> classMap = new HashMap<String, TreeSet<LongPair>>();
		private Map<Long, String> bookMap = new HashMap<Long, String>();

		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			super.setup(ctx);

			Configuration conf = ctx.getConfiguration();
			topN = conf.getInt("conf.top.n", 10);

			Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
			for (Path path : localFiles) {
				BufferedReader br = null;
				try {
					String file = path.toString();
					br = new BufferedReader(new FileReader(file));
					String line;
					String[] fields = null;
					while ((line = br.readLine()) != null) {
						fields = line.split("\\|");
						if (fields.length < 3) {
							continue;
						}

						long bookId = Long.parseLong(fields[0]);
						long freq = Long.parseLong(fields[2]);
						String classType = fields[1];
						bookMap.put(bookId, classType);
						if (classMap.containsKey(classType)) {
							classMap.get(classType).add(new LongPair(freq, bookId));
						} else {
							TreeSet<LongPair> ts = new TreeSet<LongPair>();
							ts.add(new LongPair(freq, bookId));
							classMap.put(classType, ts);
						}
					}
				} finally {
					if (br != null) {
						br.close();
					}
				}
			}   
		}

		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
			String result = null;
			for (Text value : values) {
				String s = value.toString();
				if (!"".equals(s)) {
					result = s;
				}
			}

			if (result != null) {
				ctx.write(key, new Text(result));
				return;
			}

			long bookA = key.get();
			String ct = bookMap.get(bookA);
			if (null == ct) {
				return;
			}
			TreeSet<LongPair> sps = classMap.get(ct);
			if (null == sps) {
				return ;
			}
				
			int seqNum = 0;
			StringBuffer valueOut = new StringBuffer();
			for (LongPair sp : sps) {
				long bookB = sp.getSecond();
				if (bookA == bookB) {
					continue;
				}
				seqNum++;
				valueOut.append(bookB + "||");

				if (seqNum >= topN) {
					break;
				}
			}

			if (valueOut.length() > 0) {
				valueOut.deleteCharAt(valueOut.length() - 1);
				ctx.write(key, new Text(valueOut.toString()));
			}
		
		}

	}
}
