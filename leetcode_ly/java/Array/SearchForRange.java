public class Solution {
    public int[] searchRange(int[] nums, int target) {
        int res[] = {-1,-1};
        if(nums.length == 0) return res;
        int left = 0;
        int right = nums.length - 1;
        //Ѱ����߽�
        while(left < right - 1){
            int mid = left + (right - left) / 2;
            if(nums[mid] >= target) right = mid;
            else left = mid;
        }
        if(nums[left] == target) res[0] = left;
        else if(nums[right] == target) res[0] = right;
        else return res;
        
        //Ѱ���ұ߽�
        left = 0;
        right = nums.length - 1;
        while(left < right - 1){
            int mid = left + (right - left) / 2;
            if(nums[mid] <= target) left = mid;
            else right = mid;
        }
        if(nums[right] == target) res[1] = right;
        else if(nums[left] == target) res[1] = left;
        else return res;
        
        return res;
    }
}