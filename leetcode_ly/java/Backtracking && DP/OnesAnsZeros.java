public class Solution {
    public int findMaxForm(String[] strs, int m, int n) {
        if(strs == null || strs.length == 0 || m < 0 || n < 0) return 0;
        //dp[i][j]表示i个0 j个1最多能包含几个字符串
        int[][] dp = new int[m + 1][n + 1];
        for(int i = 0; i < strs.length; i++){
            int[] res = count(strs[i]);
            for(int j = m; j >= res[0]; j--){
                for(int k = n; k >= res[1]; k--){
                    dp[j][k] = Math.max(dp[j][k],dp[j - res[0]][k - res[1]] + 1);
                }
            }
        }
        return dp[m][n];
    }
    
    public int[] count(String s){
        int[] res = new int[2];
        for(int i = 0; i < s.length(); i++){
            res[s.charAt(i) - '0']++;
        }
        return res;
    }
}