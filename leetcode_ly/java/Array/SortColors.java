public class Solution {
    public void sortColors(int[] nums) {
        int n = nums.length;
        quickSort(nums,0,n - 1);
    }
    
    public void quickSort(int[] nums,int left,int right){
        if(left > right) return;
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