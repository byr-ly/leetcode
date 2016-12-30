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