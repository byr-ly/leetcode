public class Solution {
    public int getMoneyAmount(int n) {
        if(n <= 0) return 0;
        //dp[i][j]表示从i-j需要花费的钱
        int[][] dp = new int[n + 1][n + 1];
        for(int i = 1; i <= n; i++) dp[i][i] = 0;
        for(int len = 1; len < n; len++){
            for(int i = 1; i + len <= n; i++){
                int j = i + len;
                dp[i][j] = Integer.MAX_VALUE;
                for(int k = i; k <= j; k++){
                    int a = (k - 1 < i) ? 0 : dp[i][k - 1];
                    int b = (k + 1 > j) ? 0 : dp[k + 1][j];
                    dp[i][j] = Math.min(dp[i][j],k + Math.max(a,b));
                }
            }
        }
        return dp[1][n];
    }
}