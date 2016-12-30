public class Solution {
    public int removeElement(int[] nums, int val) {
        int i = 0;
        int j = 0;
        int count = 0;
        while(j < nums.length){
            if(nums[j] == val){
                j++;
            }
            else{
                nums[i] = nums[j];
                i++;
                j++;
                count++;
            }
        }
        return count;
    }
}