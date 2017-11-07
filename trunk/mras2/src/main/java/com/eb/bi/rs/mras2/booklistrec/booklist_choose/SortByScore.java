package com.eb.bi.rs.mras2.booklistrec.booklist_choose;

import java.util.Comparator;

/**
 * Created by liyang on 2016/6/22.
 */
public class SortByScore implements Comparator {
    public int compare(Object o1, Object o2) {
        SheetScore SheetOne = (SheetScore) o1;
        SheetScore SheetTwo = (SheetScore) o2;
        double diff = SheetOne.score - SheetTwo.score;
        if (Math.abs(diff) <= 0.00001) {
            return 0;
        } else if (diff > 0) {
            return -1;
        } else {
            return 1;
        }
    }
}
