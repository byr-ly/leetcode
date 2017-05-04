public class Solution {
    public int maxCoins(int[] nums) {
        if(nums == null || nums.length == 0) return 0;
        int n = nums.length + 2;
        int[] balloons = new int[n];
        for(int i = 0; i < nums.length; i++){
            int j = i + 1;
            balloons[j] = nums[i];
        }
        balloons[0] = 1;
        balloons[n - 1] = 1;
        //dp[i][j]表示从i-j所获得的coins
        int[][] dp = new int[n][n];
        return burst(balloons,dp,0,n - 1);
    }
    
    public int burst(int[] balloons,int[][] dp,int left,int right){
        if(left + 1 == right) return 0;
        if(dp[left][right] > 0) return dp[left][right];
        int ans = 0;
        //把第i个气球当做最后一个被射的气球
        for(int i = left + 1; i < right; i++){
            ans = Math.max(ans,balloons[left] * balloons[i] * balloons[right] + burst(balloons,dp,left,i) + burst(balloons,dp,i,right));
        }
        dp[left][right] = ans;
        return ans;
    }
}