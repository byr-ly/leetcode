public class Solution {
    public int minSubArrayLen(int s, int[] nums) {
        if(nums.length == 0) return 0;
        if(nums[0] >= s) return 1;
        int start = 0;
        int end = 1;
        int sum = nums[0];
        int len = Integer.MAX_VALUE;
        while(start <= end && end < nums.length){
            sum += nums[end];
            if(sum >= s){
                while(sum >= s){
                    sum -= nums[start];
                    start++;
                }
                len = Math.min(len,end - start + 2);
            }
            end++;
        }
        return (len == Integer.MAX_VALUE) ? 0 : len;
    }
}