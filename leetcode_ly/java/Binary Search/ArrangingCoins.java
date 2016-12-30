public class Solution {
    public int arrangeCoins(int n) {
        int i = 1;
        int j = n;
        while(i <= j){
            int m = i + (j - i) / 2;
            //以下公式必须这样写，不然n * 2会越界，或者将n转化成long
            if(0.5 * m * (m + 1) <= n) i = m + 1;
            else j = m - 1;
        }
        return i - 1;
    }
}