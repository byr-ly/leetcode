public class Solution {
    public int splitArray(int[] nums, int m) {
        int max = 0;
        int sum = 0;
        for(int i = 0; i < nums.length; i++){
            max = Math.max(max,nums[i]);
            sum += nums[i];
        }
        
        int i = max;
        int j = sum;
        while(i <= j){
            int d = i + (j - i) / 2;
            if(isValid(nums,m,d)) j = d - 1;
            else i = d + 1;
        }
        return i;
    }
    
    public boolean isValid(int[] nums,int m,int d){
        int count = 1;
        int total = 0;
        for(int i = 0; i < nums.length; i++){
            total += nums[i];
            if(total > d){
                total = nums[i];
                count++;
                if(count > m) return false;
            }
        }
        return true;
    } 
}