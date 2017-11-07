package com.eb.bi.rs.mras2.andnewsrec.load_allnews;

import java.text.DecimalFormat;

/**
 * Created by LiMingji on 2016/4/13.
 */
public class WordWeight implements Comparable<WordWeight> {

    public String word;
    public Double weight;

    public WordWeight(String word, Double weight) {
        this.word = word;
        this.weight = weight;
    }

    @Override
    public int compareTo(WordWeight o) {
        if (this.word.equals(o.word)) {
            return 0;
        }
        else{
            if(this.weight.compareTo(o.weight) == 0){
                return 0;
            }
            else{
                return this.weight.compareTo(o.weight) > 0 ? -1 : 1;
            }
        }
    }

    @Override
    public String toString() {
        DecimalFormat df = new DecimalFormat("0.0000");
        return word + "," + df.format(weight);
    }
}
