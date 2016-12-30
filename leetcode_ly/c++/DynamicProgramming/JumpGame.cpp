class Solution {
public:
    bool canJump(vector<int>& nums) {
        int len = nums.size();
        if(len <= 1) return true;
        if(nums[0] >= len - 1) return true;
        
        int* canWalk = new int[len];
        canWalk[0] = nums[0];
        for(int i = 1; i < len; i++){
            int max = (canWalk[i - 1] > nums[i - 1]) ? canWalk[i - 1] : nums[i - 1];
            canWalk[i] = max - 1;
            if(canWalk[i] < 0) return false;
        }
        return canWalk[len - 1] >= 0;
    }
};