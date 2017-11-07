package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.intervectorPrep;

import com.eb.bi.rs.mras2.unifyrec.UserBookScoreTools.UserVector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * created by LiuJie on 2017/06/10.
 */
public class InterVectorBetweenUserAndBookReducer extends Reducer<Text, Text, Text, NullWritable> {

    //图书雅俗得分表
    private HashMap<String, Double> bookEleganceAndVugarityScores = null;
    //推荐图书库
    private LinkedList<PersonalizedBookVector> recommendBookList = null;
    //当前用户阅读过的分类列表
    private HashSet<String> currentUserReadClasses = null;
    //当前用户推荐过的图书列表
    private HashSet<String> currentUserRecommendList = new HashSet<String>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /**
         * 输入数据分两种。Key均为 msisdn。
         *
         * 1. 第一种值为A|用户阅读过的分类
         * 2. 第二种值为B|用户推荐列表图书
         * 3. 第三种值为C|用户偏好向量
         * */
        Iterator<Text> it = values.iterator();
        currentUserRecommendList.clear();
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
            }else if (fields[0].equals("B")) {
                currentUserRecommendList.add(fields[4]);
            }else if (fields[0].equals("C")) {
                if (fields.length < 19) {
                    System.out.printf("用户偏好向量 badline：" + currentLine);
                    return;
                }
                //C|13030657615|4|0.1|0.1147|0.0000|0.1090|0.0000|0.0000|0.0000|0.0000|0.3122|0.3407|0.0000|0.0000|458|479|811|1|13|478|13|814|812|813|
                String msisdn = fields[1];
                String class_weight = fields[3];
                String new_weight = fields[4];
                String famous_weight = fields[5];
                String serialize_weight = fields[6];
                String charge_weight = fields[7];
                String sale_weight = fields[8];
                String pack_weight = fields[9];
                String man_weight = fields[10];
                String female_weight = fields[11];
                String low_weight = fields[12];
                String high_weight = fields[13];
                String hot_weight = fields[14];
                String class1_id = fields[15];
                String class2_id = fields[16];
                String class3_id = fields[17];
                String stubborn_weight = fields[18];

                HashSet<String> simClass = new HashSet<String>();
                for (int i = 19; i < fields.length; i++) {
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
       
        if (currentUser == null||currentUserRecommendList==null||currentUserRecommendList.isEmpty()) {
            return;
        }
        /**
         * 计算当前用户和推荐书库中所有图书的交叉信息。
         */
        Collections.shuffle(this.recommendBookList);
       // System.out.println("recommendBookList size:"+recommendBookList.size());
        Iterator<PersonalizedBookVector> bookIt = this.recommendBookList.iterator();
        while (bookIt.hasNext()) {
            PersonalizedBookVector bv = bookIt.next();
            if (bv == null) {
                continue;
            }
            if(!currentUserRecommendList.contains(bv.bookID)){
            	continue;
            }
            String interVector = bv.getInterVector(currentUser, currentUserReadClasses);
            
	        StringBuffer sb = new StringBuffer(currentUser.msisdn + "|");
            sb.append(bv.bookID+"|");
            //分类|新书|性别|雅俗|连载|免费
            sb.append(interVector);
	        context.write(new Text(sb.toString()), NullWritable.get());
        }

    }

    @Override
    protected void setup(Context context) throws IOException {
       
        this.bookEleganceAndVugarityScores = new HashMap<String, Double>();
        this.recommendBookList = new LinkedList<PersonalizedBookVector>();

        Configuration conf = context.getConfiguration();
        
        bookEleganceAndVugarityScores.clear();
        recommendBookList.clear();
        
        URI[] localFiles = context.getCacheFiles();
        if (localFiles == null) {
            System.out.println("雅俗表和图书信息读取失败");
            return;
        }
        for (int i = 0; i < localFiles.length; ++i) {
            String line;
            BufferedReader in = null;
            try {
                FileSystem fs = FileSystem.get(localFiles[i], conf);
                in = new BufferedReader(new InputStreamReader(fs.open(new Path(localFiles[i]))));

                if (localFiles[i].toString().contains("classifier")) {
                    while ((line = in.readLine()) != null) {//图书book_id	雅俗得分score	日期record_day
                        String[] fields = line.split("\\|", -1);
                        if (fields.length <= 2) {
                            System.out.println("雅俗表 bad line " + fields.length + " " + line);
                            continue;
                        }
                        String bookId = fields[0];
                        String scores = fields[1];
                        this.bookEleganceAndVugarityScores.put(bookId, Double.parseDouble(scores));
                    }
                    System.out.println("雅俗表加载完成");
                }else if (localFiles[i].toString().contains("book")) {
                    //0图书bookid  3分类class_id  4付费方式charge_type 10是否完结if_finish 17图书性别sex_id   是否新书if_new
                    while ((line = in.readLine()) != null) {
                        String[] fields = line.split("\\|", -1);
                        if (fields.length < 23) {
                            System.out.println("推荐图书bad line " + fields.length + " " + line);
                            continue;
                        }
                        String bookId = fields[0];
                        String classId = fields[3];
                        String chargeType = fields[4].trim().equals("") ? "0" : fields[4];
                        String ifFinish = fields[10].trim().equals("") ? "0" : fields[10];
                        String sexId = fields[17].trim().equals("") ? "0" : fields[17];
                        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");//设置日期格式
                        String dateRecord = (fields[22].trim().equals("")||fields[22].length()!=14) ? "00000000000000" : fields[22];
                        Date d=new Date();
                        boolean after = false;
						try {
							after = df.parse(dateRecord).after(new Date(d.getTime() - 14 * 24 * 60 * 60 * 1000));
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
                 	    String ifNew = after? "0" : "1";
                        PersonalizedBookVector bv = new PersonalizedBookVector(bookEleganceAndVugarityScores, bookId, classId, ifNew,
                                chargeType, ifFinish,sexId);
                        this.recommendBookList.add(bv);
                    }
                    
                    System.out.println("推荐图书列表加载完毕");
                }
	        } finally {
	            if (in != null) {
	                in.close();
	            }
	        }
         } 
        System.out.println("图书雅俗得分表: " + bookEleganceAndVugarityScores.size());
        System.out.println("推荐图书列表: " + recommendBookList.size());
    }
}