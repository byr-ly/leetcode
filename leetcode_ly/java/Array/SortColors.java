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

public class Solution {
    public void sortColors(int[] nums) {
        if(nums == null || nums.length == 0) return;
        int i = 0;
        int j = nums.length - 1;
        int k = 0;
        for(; k < nums.length; k++){
            if(nums[k] == 0) i++;
            if(nums[k] == 2) j--;
        }
        for(int p = 0; p < i; p++){
            nums[p] = 0;
        }
        for(int p = j + 1; p < nums.length; p++){
            nums[p] = 2;
        }
        for(int p = i; p <= j; p++){
            nums[p] = 1;
        }
    }
}

void sortColors(int A[], int n) {
    int n0 = -1, n1 = -1, n2 = -1;
    for (int i = 0; i < n; ++i) {
        if (A[i] == 0) 
        {
            A[++n2] = 2; A[++n1] = 1; A[++n0] = 0;
        }
        else if (A[i] == 1) 
        {
            A[++n2] = 2; A[++n1] = 1;
        }
        else if (A[i] == 2) 
        {
            A[++n2] = 2;
        }
    }
}