public class Solution {
    public int firstMissingPositive(int[] nums) {
        Arrays.sort(nums);
        int res = 0;
        for(int i = 0; i < nums.length; i++){
            if(nums[i] <= 0) continue;
            else if(i - 1 >= 0 && nums[i] == nums[i - 1]) continue;
            else if(nums[i] - res != 1) return res + 1;
            else res++;
        }
        return res + 1;
    }
}

public class Solution {
    public int firstMissingPositive(int[] nums) {
        if(nums == null || nums.length == 0) return 1;
        for(int i = 0; i < nums.length; i++){
            int digit = nums[i];
            while(digit <= nums.length && digit > 0 && nums[digit - 1] != digit){
                swap(nums,i,digit - 1);
                digit = nums[i];
            }
        }
        int i = 0; 
        for(; i < nums.length; i++){
            if(nums[i] != i + 1) break;
        }
        return i + 1; 
    }
    
    public void swap(int[] nums,int i,int j){
        int temp = nums[i];
        nums[i] = nums[j];
        nums[j] = temp;
    }
}