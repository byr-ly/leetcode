public class Solution {
    public int maximumGap(int[] nums) {
        if(nums.length < 2) return 0;
        int res = Integer.MIN_VALUE;
        Arrays.sort(nums);
        for(int i = 1; i < nums.length; i++){
            res = Math.max(res,nums[i] - nums[i - 1]);
        }
        return res;
    }
}