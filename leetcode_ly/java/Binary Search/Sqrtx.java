public class Solution {
    public int mySqrt(int x) {
        if(x == 0) return 0;
        int i = 1;
        int j = x;
        while(i <= j){
            int m = i + (j - i) / 2;
            if((long)m * m == x) return m;
            else if((long)(m + 1) * (m + 1) == x) return m + 1;
            else if((long)m * m < x && (long)(m + 1) * (m + 1) > x) return m;
            else if((long)(m + 1) * (m + 1) < x) i = m + 1;
            else j = m - 1;
        }
        return -1;
    }
}