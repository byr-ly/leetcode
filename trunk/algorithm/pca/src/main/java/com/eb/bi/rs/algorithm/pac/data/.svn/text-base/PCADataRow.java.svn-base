package com.eb.bi.rs.algorithm.pac.data;

public class PCADataRow implements Comparable<PCADataRow> {

    int index = -1;
    int sortPos = -1;
    double pcaScore = 0D;
    static int itemNum = 0;
    double[] itemValues = null;
    String groupColValue = null;

    public PCADataRow(int idx) {
        this.index = idx;
    }

    public PCADataRow() {
    }

    public void setIndex(int idx) {
        this.index = idx;
    }

    public static void setItemNum(int col) {
        itemNum = col;
    }

    public void setItemData(double[] v) {
        this.itemValues = v;
    }

    public void setGroupColValue(String s) {
        this.groupColValue = s;
    }

    public String getGroupColValue() {
        return this.groupColValue;
    }

    public double[] getItemData() {
        return this.itemValues;
    }

    public String toString() {
        String str = String.format("%d|%.4f", new Object[]{
                Integer.valueOf(this.index), Double.valueOf(this.pcaScore)});
        return str;
    }

    public void setPCAScore(double d) {
        this.pcaScore = d;
    }

    public double getPCAScore() {
        return this.pcaScore;
    }

    public void setSortPos(int pos) {
        this.sortPos = pos;
    }

    public int getSortPos() {
        return this.sortPos;
    }

    public int compareTo(PCADataRow o) {
        if (this.index < o.index) {
            return -1;
        }
        if (this.index > o.index) {
            return 1;
        }
        return 0;
    }
}