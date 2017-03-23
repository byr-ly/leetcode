public class Solution {
    public boolean PredictTheWinner(int[] nums) {
        //dp[i][j]表示i~j段 先手比后手多拿的分数
        if(nums == null || nums.length == 0) return false;
        int[][] dp = new int[nums.length][nums.length];
        for(int i = 0; i < nums.length; i++) dp[i][i] = nums[i];
        for(int len = 1; len < nums.length; len++){
            for(int i = 0; i < nums.length - len; i++){
                int j = i + len;
                dp[i][j] = Math.max(nums[i] - dp[i + 1][j],nums[j] - dp[i][j - 1]);
            }
        }
        return dp[0][nums.length - 1] >= 0;
    }
}