public class Solution {
    public int findTargetSumWays(int[] nums, int S) {
        //若将正数组合和负数组合分别表示为P和N，则P - N = S => P - N + P + N = S + P + N => 2*P = S + sum，转化为有多少个数组子集为P
        if(nums == null || nums.length == 0) return 0;
        int sum = 0;
        for(int i : nums){
            sum += i;
        }
        return sum < S || (S + sum) % 2 != 0 ? 0 : dfs(nums,(S + sum) / 2);
    }
    
    public int dfs(int[] nums,int target){
        int[] dp = new int[target + 1];
        dp[0] = 1;
        for(int i = 1; i <= nums.length; i++){
            for(int j = target; j >= nums[i - 1]; j--){
                dp[j] += dp[j - nums[i - 1]];
            }
        }
        return dp[target];
    }
}