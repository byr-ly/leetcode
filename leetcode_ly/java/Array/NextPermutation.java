public class Solution {
    public void nextPermutation(int[] nums) {
        int i;
        for(i = nums.length - 1; i > 0; i--){
            if(nums[i] > nums[i - 1]) break;
        }
        if(i == 0){
            Arrays.sort(nums);
            return;
        }
        int j;
        for(j = i; j < nums.length; j++){
            if(nums[j] <= nums[i - 1]) break;
        }
        int temp;
        temp = nums[i - 1];
        nums[i - 1] = nums[j - 1];
        nums[j - 1] = temp;
        reverse(nums,i,nums.length - 1);
        return;
    }
    
    public void reverse(int[] nums,int start,int end){
        while(start < end){
            int temp;
            temp = nums[start];
            nums[start] = nums[end];
            nums[end] = temp;
            start++;
            end--;
        }
    }
}