public class Solution {
    public boolean search(int[] nums, int target) {
        if(nums == null || nums.length == 0) return false;
        int i = 0;
        int j = nums.length - 1;
        while(i <= j){
            int m = i + (j - i) / 2;
            if(nums[m] == target) return true;
            if(nums[m] > nums[i]){
                if(target >= nums[i] && target < nums[m]) j = m - 1;
                else i = m + 1;
            }
            else if(nums[m] < nums[i]){
                if(target <= nums[j] && target > nums[m]) i = m + 1;
                else j = m - 1;
            }
            else{
                i++;
            }
        }
        return false;
    }
}