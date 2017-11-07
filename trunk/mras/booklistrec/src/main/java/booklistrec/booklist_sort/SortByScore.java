package booklistrec.booklist_sort;


import java.util.Comparator;

/**
 * Created by liyang on 2016/4/28.
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
