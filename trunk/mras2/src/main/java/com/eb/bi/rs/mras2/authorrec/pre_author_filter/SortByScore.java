package com.eb.bi.rs.mras2.authorrec.pre_author_filter;

import java.util.Comparator;

/**
 * Created by liyang on 2016/3/14.
 */
public class SortByScore implements Comparator {
    public int compare(Object o1, Object o2) {
        Author firstAuthor = (Author) o1;
        Author secondAuthor = (Author) o2;
        double diff = firstAuthor.score - secondAuthor.score;
        if (Math.abs(diff) <= 0.00001) {
            return 0;
        } else if (diff > 0) {
            return -1;
        } else {
            return 1;
        }
    }
}
