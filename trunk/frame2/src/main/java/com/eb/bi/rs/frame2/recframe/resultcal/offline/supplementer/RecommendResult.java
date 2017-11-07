package com.eb.bi.rs.frame2.recframe.resultcal.offline.supplementer;

import java.util.ArrayList;
import java.util.List;

public class RecommendResult {
    private String in_ifhaveSuffix;
    private String inputresultformatType;

    private String out_ifhaveSuffix;
    private String outputdataformatType;

    private List<String> itemkeyList = new ArrayList<String>();
    private List<String> itemvalList = new ArrayList<String>();

    public RecommendResult() {
    }

    public List<String> getresult() {//获得推荐结果
        List<String> result = new ArrayList<String>();

        String oneResult = "";

        if (outputdataformatType.endsWith("h")) {//横表输出
            if (out_ifhaveSuffix.equals("yes")) {//带值输出

            } else {//不带值输出
                for (int i = 0; i != itemkeyList.size(); i++) {
                    oneResult += itemkeyList.get(i) + "|";
                }
                result.add(oneResult.substring(0, oneResult.length() - 1));
                oneResult = "";
            }
        } else {//竖表输出
            if (out_ifhaveSuffix.equals("yes")) {//带值输出
                for (int i = 0; i != itemkeyList.size(); i++) {
                    oneResult = itemkeyList.get(i) + "|" + itemvalList.get(i);
                    result.add(oneResult);
                }
                //result.add(oneResult);
                oneResult = "";
            } else {//不带值输出

            }
        }
        return result;
    }

    public void add(String string) {//添加推荐结果
        if (inputresultformatType.equals("h")) {//横表输入
            String[] fields = string.toString().split("\\|");
            for (int i = 0; i != fields.length; i++) {
                itemkeyList.add(fields[i]);
            }
        } else {//竖表输入
            String[] fields = string.toString().split("\\|");
            itemkeyList.add(fields[0]);
            if (fields.length != 1) {
                itemvalList.add(string.substring(fields[0].length() + 1));
            }
        }
    }

    public boolean iffiller(int recommendMinum) {//补白判断
        if (itemkeyList.size() < recommendMinum){
            return true;
        }
        return false;
    }

    public void filler(String itemKey, String itemVal) {//补白
        if (itemkeyList.contains(itemKey)){
            return;
        }
        itemkeyList.add(itemKey);
        itemvalList.add(itemVal);
    }

    public void setmodel(String inputresultformatType, String outputdataformatType,
                         String in_ifhaveSuffix, String out_ifhaveSuffix) {//数据模式设置
        this.inputresultformatType = inputresultformatType;
        this.outputdataformatType = outputdataformatType;
        this.in_ifhaveSuffix = in_ifhaveSuffix;
        this.out_ifhaveSuffix = out_ifhaveSuffix;
    }
}
