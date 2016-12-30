public class Solution {
    public int removeDuplicates(int[] nums) {
        if(nums.length == 0) return 0;
        int count = 1;
        boolean flag = true;
        for(int i = 1; i < nums.length; i++){
            if(nums[i] == nums[i - 1] && flag){
                nums[count] = nums[i];
                count++;
                flag = false;
            }
            else if(nums[i] == nums[i - 1] && !flag){
                continue;
            }
            else{
                nums[count] = nums[i];
                count++;
                flag = true;
            }
        }
        return count;
    }
}