package com.eb.bi.rs.mras.authorrec.itemcf.recommend;

import java.util.ArrayList;
import java.util.List;


public class PrefRecommItemGroup {

    //	private String classid;
    private List<RecommendItem> recommItems = null;

/*	public String getClassid()
	{
		return this.classid;
	}
	public void setClassid(String s)
	{
		this.classid = s;
	}*/

    public List<RecommendItem> getRecommendItems() {
        return this.recommItems;
    }

    public PrefRecommItemGroup() {
        recommItems = new ArrayList<RecommendItem>();
    }

    public void addItem(RecommendItem item) {
        recommItems.add(item);
    }

    public static void checkDuplicate(int idx,
                                      List<PrefRecommItemGroup> allItems, int size) {
        List<RecommendItem> currItems = allItems.get(idx).recommItems;
        if (currItems == null || currItems.size() == 0) {
            return;
        }
        if (idx == 0) {
            while (currItems.size() > size) {
                currItems.remove(size);
            }
        } else {
            for (int i = 0; i < currItems.size(); i++) {
                RecommendItem item = currItems.get(i);
                if (isDuplicateAuthor(idx, item, allItems)) {
                    currItems.remove(i);
                    i--;
                }
            }
            if (idx < 3) {
                while (currItems.size() > size) {
                    currItems.remove(size);
                }
            } else if (idx == 3) {
                int sum = getRecommItemCount(allItems, idx);
                size = 40 - sum;
                while (currItems.size() > size) {
                    currItems.remove(currItems.size() - 1);
                }
            }
        }
    }

    public static int getRecommItemCount(List<PrefRecommItemGroup> allItems, int idx) {
        int sum = 0;
        for (int i = 0; i < allItems.size(); i++) {
            if (i == idx) {
                continue;
            }
            sum += allItems.get(i).recommItems.size();
        }
        return sum;
    }

    public static boolean isDuplicateAuthor(int idx, RecommendItem item,
                                            List<PrefRecommItemGroup> allItems) {
        for (int pos = 0; pos < idx; pos++) {
            List<RecommendItem> items = allItems.get(pos).recommItems;
            for (RecommendItem checkItem : items) {
                if (checkItem.getAuthorId().equals(item.getAuthorId())) {
                    return true;
                }
            }
        }
        return false;
    }
}
