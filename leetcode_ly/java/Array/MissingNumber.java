public class Solution {
    public int missingNumber(int[] nums) {
        if(nums.length == 0) return -1;
        quickSort(nums,0,nums.length - 1);
        for(int i = 0; i < nums.length; i++){
            if(nums[i] == i) continue;
            else return i;
        }
        return nums.length;
    }
    
    public void quickSort(int[] nums,int left,int right){
        if(left < right){
            int low = left;
            int high = right;
            int target = nums[low];
            while(low < high){
                while(low < high && nums[high] >= target){
                    high--;
                }
                nums[low] = nums[high];
                while(low < high && nums[low] <= target){
                    low++;
                }
                nums[high] = nums[low];
            }
            nums[low] = target;
            quickSort(nums,left,low - 1);
            quickSort(nums,low + 1,right);
        }
    }
}