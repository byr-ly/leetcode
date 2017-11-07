package com.eb.bi.rs.algorithm.pac.data;

import java.util.Comparator;

public class PCADataRowComparator
        implements Comparator<PCADataRow> {
    public int compare(PCADataRow o1, PCADataRow o2) {
        if (o1.pcaScore > o2.pcaScore) {
            return -1;
        }
        if (o1.pcaScore < o2.pcaScore) {
            return 1;
        }
        return 0;
    }
}