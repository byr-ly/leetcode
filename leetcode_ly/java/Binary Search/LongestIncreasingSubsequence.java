public class Solution {
    public int lengthOfLIS(int[] nums) {
        if(nums.length < 2) return nums.length;
        //dp��ʾ����ǰ����ֵ����������һ����������ж೤
        int[] dp = new int[nums.length];
        for(int i = 0; i < dp.length; i++) dp[i] = 1;
        for(int i = 1; i < nums.length; i++){
            for(int j = 0; j < i; j++){
                if(nums[i] > nums[j]){
                    if(dp[j] + 1 > dp[i]) dp[i] = dp[j] + 1;
                }
            }
        }
        int res = 0;
        for(int k : dp){
            res = Math.max(k,res);
        }
        return res;
    }
}