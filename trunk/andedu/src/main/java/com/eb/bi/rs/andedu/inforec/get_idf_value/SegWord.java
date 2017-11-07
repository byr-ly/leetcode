package com.eb.bi.rs.andedu.inforec.get_idf_value;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.BaseAnalysis;
import org.ansj.splitWord.analysis.IndexAnalysis;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Created by liyang on 2016/5/31.
 */
public class SegWord {
    public static ArrayList<String> segWord(String type, String title, String content) {
        ArrayList<String> list = new ArrayList<String>();
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
            return list;
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
            if (list.contains(termsContent.get(i).getName())) {
                continue;
            } else {
                String newsWord = termsContent.get(i).getName();
                list.add(newsWord);
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
            if (list.contains(termsTitle.get(i).getName())) {
                continue;
            } else {
                String newsWord = termsTitle.get(i).getName();
                list.add(newsWord);
            }
        }
        return list;
    }
}
