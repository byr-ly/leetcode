public class Solution {
    public boolean isMatch(String s, String p) {
        if(s == null || p == null) return false;
        //dp[i][j]表示s的前i个字符是否与p的前j个字符匹配
        boolean[][] dp = new boolean[s.length() + 1][p.length() + 1];
        dp[0][0] = true;
        for(int j = 1; j <= p.length(); j++){
            dp[0][j] = dp[0][j - 1] && p.charAt(j - 1) == '*';
            for(int i = 1; i <= s.length(); i++){
                if(p.charAt(j - 1) == '?' || s.charAt(i - 1) == p.charAt(j - 1)){
                    dp[i][j] = dp[i - 1][j - 1];
                }
                else if(p.charAt(j - 1) == '*'){
                    dp[i][j] = dp[i - 1][j] || dp[i][j - 1];
                }
            }
        }
        return dp[s.length()][p.length()];
    }
}