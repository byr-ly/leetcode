package com.eb.bi.rs.mras2;

import java.util.TreeMap;

/**
 * Hello world!
 */
public class App {

    public static class CompNews implements Comparable<CompNews> {

        public String newsID;
        public Double scores;

        public CompNews(String newsID, String scores) {
            this.newsID = newsID;
            this.scores = Double.parseDouble(scores);
        }

        @Override
        public int hashCode() {
            System.out.println("hashCode");
            return newsID.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            System.out.println("equals");
            CompNews objt = (CompNews) obj;
            return this.newsID.equals(objt.newsID);
        }

        @Override
        public int compareTo(CompNews o) {
//            if (this.newsID.trim().equals(o.newsID.trim())) {
//                System.out.println(this.newsID + " " + o.newsID);
//                System.out.println("********" + this.newsID.equals(o.newsID));
//                return 0;
//            }
//            System.out.println(this.newsID + " " + o.newsID);
//            System.out.println("#######" + this.newsID.equals(o.newsID));
            if (this.newsID.equals(o.newsID)) {
                System.out.println("xxx " + this.newsID + " " + this.scores + " " + o.newsID + " " + o.scores);
                return 0;
            }else {
                int ret = this.scores.compareTo(o.scores);
                return ret > 0 ? -1 : 1;
            }
        }

        @Override
        public String toString() {
            return newsID + "|" + scores;
        }

        public static void main(String[] args) {
            TreeMap<CompNews, Double> simTopN = new TreeMap<CompNews, Double>();

            CompNews compNews1 = new CompNews("1", "1");
            CompNews compNews2 = new CompNews("2", "2");
            CompNews compNews3 = new CompNews("3", "3");
            CompNews compNews4 = new CompNews("4", "4");
            CompNews compNews5 = new CompNews("5", "5");
            CompNews compNews6 = new CompNews("1", "111");

            simTopN.put(compNews1, 1.0);
            simTopN.put(compNews2, 2.0);
            simTopN.put(compNews3, 3.0);
            simTopN.put(compNews4, 4.0);
            simTopN.put(compNews5, 5.0);
            System.out.println(simTopN);
            simTopN.put(compNews6, 6.0);
            System.out.println(simTopN);


        }
    }
}
