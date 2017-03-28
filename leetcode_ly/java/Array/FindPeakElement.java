public class Solution {
    public int findPeakElement(int[] nums) {
        if(nums.length == 0) return -1;
        if(nums.length == 1) return 0;
        if(nums[0] > nums[1]) return 0;
        for(int i = 1; i < nums.length - 1; i++){
            if(nums[i] > nums[i - 1] && nums[i] > nums[i + 1]) return i;
        }
        if(nums[nums.length - 1] > nums[nums.length - 2]) return nums.length - 1;
        return -1;
    }
}

//¶þ·Ö·¨
public class Solution {
    public int findPeakElement(int[] nums) {
        if(nums == null || nums.length == 0) return 0;
        int i = 0;
        int j = nums.length -1;
        while(i <= j){
            int m = i + (j - i) / 2; 
            int target = (m + 1 == nums.length) ? Integer.MIN_VALUE : nums[m + 1];
            if(nums[m] < target) i = m + 1;
            else j = m - 1;
        }
        return i;
    }
}