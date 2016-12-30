public class Solution {
    public boolean canJump(int[] nums) {
        int[] canWalk = new int[nums.length];
        canWalk[0] = nums[0]; 
        for(int i = 1; i < nums.length; i++){
            canWalk[i] = Math.max(canWalk[i - 1],nums[i - 1]) - 1;
            if(canWalk[i] < 0) return false;
        }
        return canWalk[nums.length - 1] >= 0;
    }
}