public class Solution {
    public int findUnsortedSubarray(int[] nums) {
        if(nums == null || nums.length == 0) return 0;
        int n  = nums.length;
        //beg--end维护需要排序的范围
        int beg = -1;int end = -2;int max = nums[0];int min = nums[n - 1];
        for(int i = 1; i < nums.length; i++){
            max = Math.max(max,nums[i]);
            min = Math.min(min,nums[n - i - 1]);
            if(nums[i] < max) end = i;
            if(nums[n - i - 1] > min) beg = n - i - 1;
        }
        return end - beg + 1;
    }
}