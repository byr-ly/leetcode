public class Solution {
    public int getSum(int a, int b) {
        //若不考虑进位，则加法可用^来实现；
        //考虑进位则将进位左移一位
        return b == 0 ? a : getSum(a^b,(a & b) << 1);
    }
}