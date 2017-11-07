package com.eb.bi.rs.algorithm.pac.data;

public class CmreadBookRow extends PCADataRow {
    private CmreadBook cmreadBook = null;

    public CmreadBookRow(int idx) {
        super(idx);
    }

    public CmreadBookRow(CmreadBook book) {
        this.cmreadBook = book;
    }

    public String toString() {
        String str = String.format("%s|%s|%.4f|%d", new Object[]{this.cmreadBook.getBookId(),
                this.groupColValue, Double.valueOf(this.pcaScore), Integer.valueOf(this.sortPos)});
        return str;
    }
}