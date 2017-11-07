package com.eb.bi.rs.mras2.authorrec.authorscore;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by liyang on 2016/3/4.
 */
public class AuthorScoreReducer extends Reducer<Text,Text,Text,NullWritable> {

    private HashMap<String,String> bookInfo;
    private HashMap<String,BookScore> authorBookInfo;
    private ArrayList<Double> scoreList;
    private HashMap<String,Integer> authorBookNum;

    @Override
    public void reduce(Text key,Iterable<Text> values,Context context)
        throws IOException,InterruptedException{
        scoreList = new ArrayList<Double>();
        authorBookInfo = new HashMap<String, BookScore>();
        for(Text val:values){
            int bookNum = 0;
            String[] fields = val.toString().split("\\|");
            if(fields.length == 2){
                if(bookInfo.containsKey(fields[0])){
                    String writer = bookInfo.get(fields[0]);
                    if(authorBookInfo.containsKey(writer)){
                        authorBookInfo.get(writer).bookNum++;
                        authorBookInfo.get(writer).scoreList.add(Double.parseDouble(fields[1]));
                    }
                    else {
                        String score = fields[1];
                        bookNum++;
                        scoreList.add(Double.parseDouble(score));
                        BookScore bookScore = new BookScore(bookNum, scoreList);
                        authorBookInfo.put(writer,bookScore);
                    }
                }
            }
        }
        //遍历AuthorBookInfo，取得用户读同一作家的图书本书
        Iterator iter = authorBookInfo.entrySet().iterator();
        while(iter.hasNext()){
            Map.Entry entry = (Map.Entry) iter.next();
            String writer = (String) entry.getKey();
            BookScore bookScore = authorBookInfo.get(writer);
            int bookNum = bookScore.bookNum;
            double maxScore = getMaxScore(scoreList);
            double aveTopN = topNAverage(scoreList);
            double variation = getVariation(scoreList);
            switch (bookNum){
                case 1:
                    if(bookScore.scoreList.size() == 1) {
                        double score = bookScore.scoreList.get(0);
                        //得分小于5或（得分等于5且上架书数量为1），则打分为score
                        if ((score < 5) || (score == 5 && authorBookNum.get(writer) == 1)) {
                            context.write(new Text(key + "|" + writer + "|" + score), NullWritable.get());
                        }
                        //得分等于5且上架书数量大于1，则打分为score - 0.5
                        else if(score == 5 && authorBookNum.get(writer) > 1){
                            context.write(new Text(key + "|" + writer + "|" + (score - 0.5)),NullWritable.get());
                        }
                    }
                    break;
                case 2:
                    double minScore = getMinScore(scoreList);
                    //最大得分小于5或（最大得分等于5且最小得分>=3.5），则得分为最大得分
                    if((maxScore < 5) || (maxScore == 5 && minScore >= 3.5)){
                        context.write(new Text(key + "|" + writer + "|" + maxScore), NullWritable.get());
                    }
                    //最大得分等于5且最小得分小于3.5，则得分为最大得分 - 0.5
                    else if(maxScore == 5 && minScore < 3.5){
                        context.write(new Text(key + "|" + writer + "|" + (maxScore - 0.5)), NullWritable.get());
                    }
                    break;
                case 3:
                case 4:
                    //得分为5的书所占比例>=0.5，则得分为最大得分
                    if(aveTopN >= 0.5){
                        context.write(new Text(key + "|" + writer + "|" + maxScore), NullWritable.get());
                    }
                    //得分为5的书所占比例<0.5 且标准差/平均值<1，得分为max_score-(variation)^log(2,n)
                    else if(aveTopN < 0.5 && variation < 1){
                        double value = maxScore - Math.pow(variation,Math.log(scoreList.size()) / Math.log(2));
                        context.write(new Text(key + "|" + writer + "|" + value),NullWritable.get());
                    }
                    //得分为5的书所占比例<0.5 且标准差/平均值>=1，得分为max_score-(variation)^(log(2,n)/n)
                    else if(aveTopN < 0.5 && variation >= 1){
                        double value = maxScore - Math.pow(variation,(Math.log(scoreList.size())
                                / Math.log(2)) / scoreList.size());
                        context.write(new Text(key + "|" + writer + "|" + value), NullWritable.get());
                    }
                    break;
                    default:
                        double aveSecN = secNAverage(scoreList);
                        //得分4-5的书所占比例>=0.5 且得分为5的书所占比例>=0.3，则得分为最大得分
                        if(aveSecN >= 0.5 && aveTopN >= 0.3){
                            context.write(new Text(key + "|" + writer + "|" + maxScore), NullWritable.get());
                        }
                        //得分4-5的书所占比例>=0.5 或得分为5的书所占比例>=0.3 且标准差/平均值<1，则得分为max_score-(variation)^log(2,n)
                        else if((aveSecN >= 0.5 || aveTopN >= 0.3) && variation < 1){
                            double value = maxScore - Math.pow(variation,Math.log(scoreList.size()) / Math.log(2));
                            context.write(new Text(key + "|" + writer + "|" + value), NullWritable.get());
                        }
                        //得分4-5的书所占比例>=0.5 或得分为5的书所占比例>=0.3 且标准差/平均值>=1，则得分为max_score-(variation)^(log(2,n)/n)
                        else if((aveSecN >= 0.5 || aveTopN >= 0.3) && variation >= 1){
                            double value = maxScore - Math.pow(variation,(Math.log(scoreList.size())
                                    / Math.log(2)) / scoreList.size());
                            context.write(new Text(key + "|" + writer + "|" + value), NullWritable.get());
                        }
                        break;
            }

        }

    }

    protected void setup(Context context) throws IOException, InterruptedException {

        bookInfo = new HashMap<String, String>();
        authorBookNum = new HashMap<String, Integer>();

        System.out.printf("reduce setup");
        Configuration conf = context.getConfiguration();
        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

        if (localFiles == null) {
            System.out.println("没有找到图书信息File ");
            return;
        }
        for (int i = 0; i < localFiles.length; i++) {
            System.out.println("localFile: " + localFiles[i]);
            String line;
            BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));

            String fileName = localFiles[i].toString();
            if (fileName.contains("0000")) {
                //图书  | 作家
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    if (fields.length < 2) {
                        continue;
                    }
                    String book = fields[0];
                    String writer = fields[1];
                    if (!bookInfo.containsKey(book)) {
                        bookInfo.put(book,writer);
                    }
                    if(authorBookNum.containsKey(writer)){
                        Integer sumNum = authorBookNum.get(writer);
                        sumNum++;
                        authorBookNum.put(writer,sumNum);
                    }
                    else{
                        authorBookNum.put(writer,1);
                    }
                }
                br.close();
                System.out.println("图书信息列表加载成功 " + bookInfo.size());
                System.out.println("作家-图书本数列表加载成功" + authorBookNum.size());
            }
        }
    }

    //获得最大得分
    public double getMaxScore(ArrayList<Double> scoreList){
        double maxScore = scoreList.get(0);
        for(int i = 1; i < scoreList.size(); i++){
            if(maxScore < scoreList.get(i)){
                maxScore = scoreList.get(i);
            }
        }
        return maxScore;
    }

    //获得最小得分
    public double getMinScore(ArrayList<Double> scoreList){
        double minScore = scoreList.get(0);
        for(int i = 1; i < scoreList.size(); i++){
            if(minScore > scoreList.get(i)){
                minScore = scoreList.get(i);
            }
        }
        return minScore;
    }

    //打分为5的书占的比例
    public double topNAverage(ArrayList<Double> scoreList){
        int topN = 0;
        for(int j = 0; j < scoreList.size(); j++){
            if(scoreList.get(j) == 5){
                topN++;
            }
        }
        try {
            return topN / scoreList.size();
        }catch (ArithmeticException e){
            System.out.printf("用户没有读过该作家的书");
            return 0;
        }
    }

    //打分>=4 <=5的书占的比例
    public double secNAverage(ArrayList<Double> scoreList){
        int secN = 0;
        for(int j = 0; j < scoreList.size(); j++){
            if(scoreList.get(j) >= 4 && scoreList.get(j) <= 5){
                secN++;
            }
        }
        try {
            return secN / scoreList.size();
        }catch (ArithmeticException e){
            System.out.printf("用户没有读过该作家的书");
            return 0;
        }
    }

    //用户阅读过的某个作家下面图书打分的标准差除以图书的平均分
    public double getVariation(ArrayList<Double> scoreList){
        double sumScore = 0;
        for(int j = 0; j < scoreList.size(); j++){
            sumScore += scoreList.get(j);
        }
        double aveScore = 0;
        try{
            aveScore = sumScore / scoreList.size();
        }catch (ArithmeticException e){
            System.out.printf("用户没有读过该作家的书");
        }

        double sum = 0;
        for(int k = 0; k < scoreList.size(); k++){
            sum += Math.pow((scoreList.get(k) - aveScore), 2);
        }
        try {
            return Math.sqrt(sum / scoreList.size());
        }catch (ArithmeticException e) {
            System.out.printf("用户没有读过该作家的书");
            return 0;
        }
    }
}
