public class Solution {
    public int arrangeCoins(int n) {
        int i = 1;
        int j = n;
        while(i <= j){
            int m = i + (j - i) / 2;
            //���¹�ʽ��������д����Ȼn * 2��Խ�磬���߽�nת����long
            if(0.5 * m * (m + 1) <= n) i = m + 1;
            else j = m - 1;
        }
        return i - 1;
    }
}