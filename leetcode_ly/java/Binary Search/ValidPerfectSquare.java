public class Solution {
    public boolean isPerfectSquare(int num) {
        int i = 1;
        int j = num;
        while(i <= j){
            int m = i + (j - i) / 2;
            if((long)m * m == (long)num) return true;
            else if((long)m * m < (long)num) i = m + 1;
            else j = m - 1;
        }
        return false;
    }
}