package com.eb.bi.rs.mras.unifyrec.attributeengin.SeparatePrefRecResult;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.eb.bi.rs.mras.unifyrec.attributeengin.UserBookScoreTools.UserVector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by LiMingji on 2015/11/10.
 * Revised by LiuJie on 2016/04/10.
 */
public class SeparatePrefRecResultReducer extends Reducer<Text, Text, Text, NullWritable> {

    public static boolean debug = false;

    //图书雅俗得分表
    private HashMap<String, Double> bookEleganceAndVugarityScores = null;
    //推荐图书库
    private LinkedList<SeparateBookVector> recommendBookList = null;
    //当前用户阅读过的分类列表
    private HashSet<String> currentUserReadClasses = null;
    //当前用户阅读过的历史图书列表
    private HashSet<String> currentUserReadHistory = new HashSet<String>();
    
    //存放推荐图书
    private TreeSet<String> tops;


    private Comparator<String> topNComp = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            String[] fields1 = o1.split(",");
            String bookID1 = fields1[0];
            Double scores1 = parseDouble(fields1[1]);

            String[] fields2 = o2.split(",");
            String bookID2 = fields2[0];
            Double scores2 = parseDouble(fields2[1]);

            if (bookID1.equals(bookID2)) {
                return 0;
            }
            if (scores1 > scores2) {
                return -1;
            }else {
                return 1;
            }
        }
    };

    private double parseDouble(String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            //do nothing
        }
        return 0.0;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {   	
        /**
         * 输入数据分两种。Key均为 msisdn。
         *
         * 1. 第一种值为A|用户阅读过的分类
         * 2. 第二种值为B|用户阅读过的历史图书
         * 3. 第三种值为C|用户偏好向量。
         * */
    	SeparatePrefRecResultReducer.debug = context.getConfiguration().getBoolean("separate_prefrec_result_reduce_debug", false);

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
            } else if (fields[0].equals("B")) {
                 if (fields.length < 1) {
                	 currentUserReadHistory.clear();
                     for (int i = 1; i < fields.length; i++) {
                         String history = fields[i].trim();
                         if (!history.equals("")) {
                         	currentUserReadHistory.add(history);
                         }
                     }
                }            
            } else if (fields[0].equals("C")) {
                if (fields.length < 19) {
                	System.out.printf("用户偏好向量 badline："+ currentLine);
                    return;
                }
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
        if (currentUser == null) {
            return;
        }

        debug = debug && currentUser.msisdn.trim().equals("13858038966");
        if(debug) {
            System.out.println(currentUser);
            System.out.println("用户阅读过的分类： " + currentUserReadClasses);
        }
        /**
         * 计算当前用户和推荐书库中所有图书的得分。
         */
        tops = new TreeSet<String>(topNComp);
        Collections.shuffle(this.recommendBookList);
        Set<String> picked = new HashSet<String>();
        picked.clear();
        Iterator<SeparateBookVector> bookIt = this.recommendBookList.iterator();
        while (bookIt.hasNext()) {
            SeparateBookVector bv = bookIt.next();
            if (bv == null) {
                continue;
            }
			if (!currentUserReadHistory.isEmpty() &&currentUserReadHistory.contains(bv.bookID)) {
                continue;
            }
            if(debug) {
               System.out.println(bv.toString());
            }   
            if(picked.contains(bv.bookID)){
				continue;
			}
			picked.add(bv.bookID);
            double weight=0.0;
            weight = bv.getWeight(currentUser, currentUserReadClasses);           
            DecimalFormat dcmFmt = new DecimalFormat("0.0000");
        	//if (weight > 0.0) {        
            	tops.add(bv.bookID + "," + dcmFmt.format(weight));              
            //}
        }
        StringBuffer sb = new StringBuffer(currentUser.msisdn + ";");
        for (String value : tops) {
            sb.append(value);
            sb.append("|");
        }
        context.write(new Text(sb.toString()), NullWritable.get());  
        
    }     
    @Override
    protected void setup(Context context) throws IOException {
        
        Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        if (localFiles == null) {
            System.out.println("用户阅读过的分类信息读取失败");
            return;
        }
        this.bookEleganceAndVugarityScores = new HashMap<String, Double>();
        this.recommendBookList = new LinkedList<SeparateBookVector>();
        this.currentUserReadClasses = new HashSet<String>();

        for (int i = 0; i < localFiles.length; i++) {
            //图书book_id	雅俗得分score	日期record_day
            String line;
            BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));
            String fileName = localFiles[i].toString();
            if (fileName.contains("classifier")) {
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|", -1);
                    if (fields.length <= 2) {
                        System.out.println("雅俗表 bad line " + fields.length + " " + line);
                        continue;
                    }
                    String bookId = fields[0];
                    String scores = fields[1];
                    this.bookEleganceAndVugarityScores.put(bookId, Double.parseDouble(scores));
                }
                br.close();
                System.out.println("雅俗表加载完成");
            } else if (fileName.contains("recom_bookinfo")) {
                //图书bookid	 分类class_id 是否新书if_new	是否名家if_editfame	付费方式charge_type	是否完结if_finish	是否流行书if_hotrec	图书性别sex_id
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|", -1);
                    if (fields.length < 8) {
                        System.out.println("推荐图书bad line " + fields.length + " " + line);
                        continue;
                    }
                    String bookId = fields[0];
                    String classId = fields[1];
                    String ifNew = fields[2].trim().equals("") ? "0" : fields[2];
                    String ifEditFame = fields[3].trim().equals("") ? "0" : fields[3];
                    String chargeType = fields[4].trim().equals("") ? "0" : fields[4];
                    String ifFinish = fields[5].trim().equals("") ? "0" : fields[5];
                    String ifHotRec = fields[6].trim().equals("") ? "0" : fields[6];
                    String sexId = fields[7].trim().equals("") ? "0" : fields[7];
                    SeparateBookVector bv = new SeparateBookVector(bookEleganceAndVugarityScores, bookId, classId, ifNew, ifEditFame,
                            chargeType, ifFinish, ifHotRec, sexId);
                    this.recommendBookList.add(bv);
                }
                br.close();
                bookEleganceAndVugarityScores.clear();
                System.out.println("推荐图书列表加载完毕");
            }
        }
        System.out.println("图书雅俗得分表: " + bookEleganceAndVugarityScores.size());
        System.out.println("推荐图书列表: " + recommendBookList.size());
    }
}