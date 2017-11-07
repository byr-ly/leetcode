package com.eb.bi.rs.andedu.inforec.getRetsAndFilter;

import java.util.Comparator;

/**
 * Created by liysng on 2016/7/21.
 */
public class SortByScore implements Comparator {
    public int compare(Object o1, Object o2) {
        CompInfos NewsOne = (CompInfos) o1;
        CompInfos NewsTwo = (CompInfos) o2;
        if(NewsOne.newsID.equals(NewsTwo.newsID)){
            return 0;
        }
        else{
            if(NewsOne.scores.compareTo(NewsTwo.scores) == 0){
                return 0;
            }
            else{
                return NewsOne.scores.compareTo(NewsTwo.scores) > 0 ? -1 : 1;
            }
        }
    }
}