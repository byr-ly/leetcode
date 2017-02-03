public class Solution {
    public int rob(int[] nums) {
        //即抢了第一家就不能抢最后一家，则计算第二家到最后一家和第一家到倒数第二家两个结果的最大值
        if(nums == null || nums.length == 0) return 0;
        if(nums.length == 1) return nums[0];
        System.out.println(rob(nums,0,nums.length - 2));
        System.out.println(rob(nums,1,nums.length - 1));
        return Math.max(rob(nums,0,nums.length - 2),rob(nums,1,nums.length - 1));
    }
    
    public int rob(int[] nums,int start,int end){
        if(start == end) return nums[start];
        if(end - start == 1) return Math.max(nums[start],nums[end]);
        int length = end - start + 1;
        int[] dp = new int[length];
        dp[0] = nums[start];
        dp[1] = Math.max(nums[start],nums[start + 1]);
        for(int i = 2; i < length; i++){
            //两次调用数组挪了一位
            if(start == 0) dp[i] = Math.max(nums[i] + dp[i - 2],dp[i - 1]);
            else dp[i] = Math.max(nums[i + 1] + dp[i - 2],dp[i - 1]);
        }
        return dp[length - 1];
    }
}