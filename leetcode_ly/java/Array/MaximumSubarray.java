public class Solution {
    public int maxSubArray(int[] nums) {
        if(nums.length == 0) return -1;
        if(nums.length == 1) return nums[0];
        int max = nums[0];
        int sum = nums[0];
        for(int i = 1; i < nums.length; i++){
            sum = (sum < 0) ? nums[i] : (nums[i] + sum);
            max = Math.max(max,sum);
        }
        return max;
    }
}