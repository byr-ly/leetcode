package com.eb.bi.rs.mras.authorrec.itemcf.bookinfo;

public class BookInfo {

    private String bookName;
    private String class_name;
    private String contentStatus = "";
    private String butype;
    private AuthorInfo author;

    public BookInfo() {
    }

    public BookInfo(String name, String cname, String status, String bu) {
        this.bookName = name;
        this.class_name = cname;
        this.contentStatus = status;
        this.butype = bu;
    }

    public void setAuthor(AuthorInfo a) {
        this.author = a;
    }

    public String toString() {
        String str = String.format("%s|%s|%s|%s|%s", bookName, class_name,
                contentStatus, butype, author.toString());
        return str;
    }

}
