class Solution {
public:
    int findMin(vector<int>& nums) {
        if(nums.size() == 0) return 0;
        for(int i = 1; i < nums.size(); i++){
            if(nums[i] >= nums[i - 1]) continue;
            else return nums[i];
        }
        return nums[0];
    }
};