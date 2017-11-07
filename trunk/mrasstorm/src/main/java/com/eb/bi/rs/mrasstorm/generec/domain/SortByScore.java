package com.eb.bi.rs.mrasstorm.generec.domain;


import java.util.Comparator;

/**
 * Created by liyang on 2016/7/7.
 */
public class SortByScore implements Comparator {
    public int compare(Object o1, Object o2) {
        BookGeneClassInfo SheetOne = (BookGeneClassInfo) o1;
        BookGeneClassInfo SheetTwo = (BookGeneClassInfo) o2;
        double diff = SheetOne.getScore() - SheetTwo.getScore();
        if (Math.abs(diff) <= 0.00001) {
            return 0;
        } else if (diff > 0) {
            return -1;
        } else {
            return 1;
        }
    }
}
