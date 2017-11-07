package com.eb.bi.rs.andedu.inforec.infos_word_transpose;//package com.eb.bi.rs.mras2.andnewsrec.news_word_transpose;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//
//import java.io.IOException;
//
///**
// * Created by LiMingji on 2016/3/21.
// */
//public class NewsWordsTransposeDriver {
//    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        if (args.length != 2) {
//            System.out.println("Args Error!");
//            return ;
//        }
//
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "NewsWordsTranspose");
//        job.setJarByClass(NewsWordsTransposeDriver.class);
//
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//        job.setMapperClass(NewsWordsTransposeMapper.class);
//        job.setReducerClass(NewsWordsTransposeReducer.class);
//
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//        job.waitForCompletion(true);
//    }
//}
