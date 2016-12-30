public class Solution {
    public void merge(int[] nums1, int m, int[] nums2, int n) {
        for(int i = m; i < m + n; i++){
            nums1[i] = nums2[i - m];
        }
        quickSort(nums1,0,m + n - 1);
        return;
    }
    
    public void quickSort(int[] nums,int left,int right){
        if(left >= right) return;
        int low = left;
        int high = right;
        int target = nums[low];
        //if(low < high){
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
        //}
    }
}