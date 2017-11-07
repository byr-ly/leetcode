package com.eb.bi.rs.mras2.andnewsrec.load_allnews;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.BaseAnalysis;
import org.ansj.splitWord.analysis.IndexAnalysis;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * 分词工具类
 * Created by LiMingji on 2016/3/14.
 */
public class SegMore {
    public static Map<String, NewsWord> segMore(String type, String title, String content) {
        Map<String, NewsWord> map = new HashMap<String, NewsWord>();
        LinkedList<Term> termsTitle = null;
        LinkedList<Term> termsContent = null;

        //BaseAnalysis,ToAnalysis,NlpAnalysis,IndexAnalysis
        if (type.equals("B")) {
            termsTitle = (LinkedList<Term>) BaseAnalysis.parse(title);
            termsContent = (LinkedList<Term>) BaseAnalysis.parse(content);
        } else if (type.equals("T")) {
            termsTitle = (LinkedList<Term>) ToAnalysis.parse(title);
            termsContent = (LinkedList<Term>) ToAnalysis.parse(content);
        } else if (type.equals("N")) {
            termsTitle = (LinkedList<Term>) NlpAnalysis.parse(title);
            termsContent = (LinkedList<Term>) NlpAnalysis.parse(content);
        } else if (type.equals("I")) {
            termsTitle = (LinkedList<Term>) IndexAnalysis.parse(title);
            termsContent = (LinkedList<Term>) IndexAnalysis.parse(content);
        } else {
            return map;
        }

        for (int i = 0; i < termsContent.size(); i++) {
            // 正文取名词,动词,形容词
            if (!termsContent.get(i).getNatureStr().startsWith("n") && !termsContent.get(i).getNatureStr().startsWith("v")
                    && !termsContent.get(i).getNatureStr().startsWith("a")) {
                continue;
            }
            if (termsContent.get(i).getName().isEmpty()) {
                continue;
            }
            if (map.containsKey(termsContent.get(i).getName())) {
                NewsWord newsWord = map.get(termsContent.get(i).getName());
                newsWord.times += 1;
                map.put(termsContent.get(i).getName(), newsWord);
            } else {
                NewsWord newsWord = new NewsWord(termsContent.get(i).getName());
                map.put(termsContent.get(i).getName(), newsWord);
            }
        }

        for (int i = 0; i < termsTitle.size(); i++) {
            // 标题取名词,动词,形容词
            if (!termsTitle.get(i).getNatureStr().startsWith("n") && !termsTitle.get(i).getNatureStr().startsWith("v")
                    && !termsTitle.get(i).getNatureStr().startsWith("a")) {
                continue;
            }
            if (termsTitle.get(i).getName().isEmpty()) {
                continue;
            }
            if (map.containsKey(termsTitle.get(i).getName())) {
                NewsWord newsWord = map.get(termsTitle.get(i).getName());
                newsWord.times += 1;
                map.put(termsTitle.get(i).getName(), newsWord);
            } else {
                NewsWord newsWord = new NewsWord(termsTitle.get(i).getName());
                map.put(termsTitle.get(i).getName(), newsWord);
            }
        }
        return map;
    }
}
