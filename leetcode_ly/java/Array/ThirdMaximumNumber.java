public class Solution {
    public int thirdMax(int[] nums) {
        Arrays.sort(nums);
        int count = 0;
        int i;
        for(i = nums.length - 1; i > 0; i--){
            if(nums[i] != nums[i - 1]) count++;
            if(count == 2) break;
        }
        return (count == 2) ? nums[i - 1] : nums[nums.length - 1];
    }
}