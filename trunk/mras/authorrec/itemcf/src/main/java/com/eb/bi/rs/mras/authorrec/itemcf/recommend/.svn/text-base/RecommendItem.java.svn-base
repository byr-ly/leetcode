package com.eb.bi.rs.mras.authorrec.itemcf.recommend;


public class RecommendItem implements Comparable<RecommendItem> {

    private String authorid;
    private String bookid;
    private double score;

    public RecommendItem(String aid, String bid, double sc) {
        this.authorid = aid;
        this.bookid = bid;
        this.score = sc;
    }


    public String getBookId() {
        return this.bookid;
    }

    public String getAuthorId() {
        return this.authorid;
    }

    public double getScore() {
        return this.score;
    }


    @Override
    public int compareTo(RecommendItem o) {
        // TODO Auto-generated method stub
        if (o.score > this.score) {
            return 1;
        } else if (o.score < this.score) {
            return -1;
        }
        return 0;
    }

    public String toString() {
        String str = null;
        if (score <= 0) {
            str = String.format("%s|%s|0", authorid, bookid);
        } else {
            str = String.format("%s|%s|%.4f", authorid, bookid, score);
        }
        return str;
    }

    public RecommendItem clone() {
        RecommendItem newItem = new RecommendItem(
                this.authorid, this.bookid, this.score);
        return newItem;
    }

}
