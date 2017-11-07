package com.eb.bi.rs.algorithm.pac.data;

public class CmreadBook {
    private String bookId = null;
    private String bookName = null;
    private String chargeType = null;

    public CmreadBook(String bookid, String name, String ctype) {
        this.bookId = bookid;
        this.bookName = name;
        this.chargeType = ctype;
    }

    public CmreadBook(String bookid, String name) {
        this.bookId = bookid;
        this.bookName = name;
    }

    public String getBookId() {
        return this.bookId;
    }

    public String getBooName() {
        return this.bookName;
    }

    public String getChargeType() {
        return this.chargeType;
    }
}