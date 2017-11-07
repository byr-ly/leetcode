package com.eb.bi.rs.mras2.unifyrec.ForceRecClassPref;

import com.eb.bi.rs.mras2.unifyrec.UserBookScoreTools.UserVector;
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
import java.util.*;

/**
 * Created by LiuJie on 2016/04/10.
 */
public class ForceRecClassPrefReducer extends Reducer<Text, Text, Text, NullWritable> {

    public static double class_limit = 0.0;
    //推荐图书库
    private LinkedList<ForceRecBookVector> recommendBookList = null;
    //当前用户阅读过的分类列表
    private HashSet<String> currentUserReadClasses = null;
    //存放分类分大于0.5分的推荐图书
    private TreeSet<String> tops1,tops2,tops3;


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {   	
        /**
         * 输入数据分两种。Key均为 msisdn。
         *
         * 1. 第一种值为A|用户阅读过的分类
         * 2. 第二种值为C|用户偏好向量。
         * */
    	Configuration conf = context.getConfiguration();
    	ForceRecClassPrefReducer.class_limit = Double.parseDouble(conf.get("forcerec_classpref_reduce_class_limit","0.5"));
        Iterator<Text> it = values.iterator();

        //当前用户
        UserVector currentUser = null;
        while (it.hasNext()) {	

            String currentLine = it.next().toString();        
            String[] fields = currentLine.split("\\|", -1);
            if (fields[0].equals("A")) {
                if (currentUserReadClasses == null) {
                    currentUserReadClasses = new HashSet<String>();
                }
                currentUserReadClasses.clear();
                for (int i = 2; i < fields.length; i++) {
                    currentUserReadClasses.add(fields[i]);
                }
            } else if (fields[0].equals("C")) {
                if (fields.length < 18) {
                	System.out.printf("用户偏好向量 badline："+ currentLine);
                    return;
                }
                 String msisdn = fields[1];
                 String class_weight = fields[2];
                 String new_weight = fields[3];
                 String famous_weight = fields[4];
                 String serialize_weight = fields[5];
                 String charge_weight = fields[6];
                 String sale_weight = fields[7];
                 String pack_weight = fields[8];
                 String man_weight = fields[9];
                 String female_weight = fields[10];
                 String low_weight = fields[11];
                 String high_weight = fields[12];
                 String hot_weight = fields[13];
                 String class1_id = fields[14];
                 String class2_id = fields[15];
                 String class3_id = fields[16];
                 String stubborn_weight = fields[17];

                HashSet<String> simClass = new HashSet<String>();
                for (int i = 18; i < fields.length; i++) {
                    String classes = fields[i].trim();
                    if (!classes.equals("")) {
                        simClass.add(classes);
                    }
                }
               
                currentUser = new UserVector(msisdn, class_weight, new_weight, famous_weight,
                        serialize_weight, charge_weight, sale_weight, pack_weight
                        , man_weight, female_weight, low_weight, high_weight, hot_weight
                        , class1_id, class2_id, class3_id, stubborn_weight, simClass);
            }
        }
        if (currentUser == null) {
            return;
        }

        /**
         * 计算当前用户对推荐书库中所有图书的分类偏好分。
         */
        tops1 = new TreeSet<String>();tops2 = new TreeSet<String>();tops3 = new TreeSet<String>();
        tops1.clear();tops2.clear();tops3.clear();
        Collections.shuffle(this.recommendBookList);
        
        Iterator<ForceRecBookVector> bookIt = this.recommendBookList.iterator();
        while (bookIt.hasNext()) {
        	ForceRecBookVector bv = bookIt.next();
            if (bv == null) {
                continue;
            }              	    
            double weight=0.0;
            weight = bv.getClassWeight(currentUser, currentUserReadClasses);           
            if (weight > class_limit) {
        		if(bv.pageId.equals("1")){
        			tops1.add(bv.bookID);
        		}else if(bv.pageId.equals("2")){
        			tops2.add(bv.bookID);
        		}else if(bv.pageId.equals("3")){
        			tops3.add(bv.bookID);
        		}	
            }
        } 
        StringBuffer sb1 = new StringBuffer(currentUser.msisdn + "_1;");
        StringBuffer sb2 = new StringBuffer(currentUser.msisdn + "_2;");
        StringBuffer sb3 = new StringBuffer(currentUser.msisdn + "_3;");
        for (String value1 : tops1) {
            sb1.append(value1+"|");
        }  
        for (String value2 : tops2) {
            sb2.append(value2+"|");
        } 
        for (String value3 : tops3) { 	
            sb3.append(value3+"|");     
        } 
        context.write(new Text(sb1.toString()), NullWritable.get());
        context.write(new Text(sb2.toString()), NullWritable.get());
        context.write(new Text(sb3.toString()), NullWritable.get());
        
    }     
    @Override
    protected void setup(Context context) throws IOException {
    	
        //Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
//        if (localFiles == null) {
//            System.out.println("分版编辑强推库图书读取失败");
//            return;
//        }
        
        this.recommendBookList = new LinkedList<ForceRecBookVector>();

        System.out.printf("reduce setup");
        Configuration conf = context.getConfiguration();
//        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

        URI[] localFiles = context.getCacheFiles();
        if (localFiles == null) {
            System.out.println("分版编辑强推库图书读取失败");
            return;
        }
        for (int i = 0; i < localFiles.length; ++i) {
            String line;
            BufferedReader in = null;
            try {
                /*DistributedCache修改点*/
                FileSystem fs = FileSystem.get(localFiles[i], conf);
                in = new BufferedReader(new InputStreamReader(fs.open(new Path(localFiles[i]))));

                if (localFiles[i].toString().contains("man")) {
                    while ((line = in.readLine()) != null) {//图书bookid
                        String [] fields =line.split("\\|", -1);
                        String bookId = fields[0];
                        String classId = fields[1];
                        String pageId ="1";
                        ForceRecBookVector bv = new ForceRecBookVector(bookId, classId, pageId);
                        this.recommendBookList.add(bv);
                    }
                    System.out.println("男版推荐图书列表加载完毕");
                } else if (localFiles[i].toString().contains("female")) {
                    while ((line = in.readLine()) != null) {
                        String [] fields =line.split("\\|", -1);
                        String bookId = fields[0];
                        String classId = fields[1];
                        String pageId ="2";
                        ForceRecBookVector bv = new ForceRecBookVector(bookId, classId, pageId);
                        this.recommendBookList.add(bv);
                    }
                    System.out.println("女版推荐图书列表加载完毕");
                }else if (localFiles[i].toString().contains("publish")) {
                    while ((line = in.readLine()) != null) {
                        String [] fields =line.split("\\|", -1);
                        String bookId = fields[0];
                        String classId = fields[1];
                        String pageId ="3";
                        ForceRecBookVector bv = new ForceRecBookVector(bookId, classId, pageId);
                        this.recommendBookList.add(bv);
                    }
                    System.out.println("出版推荐图书列表加载完毕");
                }
            } finally {
                if (in != null) {
                    in.close();
                }
            }
        }
        System.out.println("所有分版推荐图书列表: " + recommendBookList.size());
    }
    
}