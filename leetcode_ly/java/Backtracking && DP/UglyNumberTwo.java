public class Solution {
    public int nthUglyNumber(int n) {
        if(n == 1) return 1;
        int[] dp = new int[n];
        dp[0] = 1;
        int first = 0;int second = 0;int third = 0;
        int factor2 = 2;int factor3 = 3;int factor5 = 5;
        for(int i = 1; i < n; i++){
            dp[i] = Math.min(Math.min(factor2,factor3),factor5);
            if(dp[i] == factor2){
                factor2 = 2 * dp[++first];
            }
            if(dp[i] == factor3){
                factor3 = 3 * dp[++second];
            }
            if(dp[i] == factor5){
                factor5 = 5 * dp[++third];
            }
        }
        return dp[n - 1];
    }
}