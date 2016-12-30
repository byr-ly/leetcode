class Solution {
public:
    int maxSubArray(vector<int>& nums) {
        int max = INT_MIN;
        int sum = INT_MIN;
        for(int i = 0; i < nums.size(); i++){
            sum = (sum < 0) ? nums[i] : (sum + nums[i]);
            max = (sum > max) ? sum : max;
        }
        return max;
    }
};