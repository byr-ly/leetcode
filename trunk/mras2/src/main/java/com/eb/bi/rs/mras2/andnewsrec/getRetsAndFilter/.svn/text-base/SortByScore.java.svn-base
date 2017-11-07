package com.eb.bi.rs.mras2.andnewsrec.getRetsAndFilter;

import java.util.Comparator;

/**
 * Created by liysng on 2016/7/21.
 */
public class SortByScore implements Comparator {
    public int compare(Object o1, Object o2) {
        CompNews NewsOne = (CompNews) o1;
        CompNews NewsTwo = (CompNews) o2;
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