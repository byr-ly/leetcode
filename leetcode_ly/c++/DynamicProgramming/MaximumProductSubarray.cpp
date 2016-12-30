class Solution {
public:
    int maxProduct(vector<int>& nums) {
        if(nums.size() == 0) return 0;
        if(nums.size() == 1) return nums[0];
        int max_temp = nums[0];
        int min_temp = nums[0];
        int max = nums[0];
        for(int i = 1; i < nums.size(); i++){
            int a = nums[i] * max_temp;
            int b = nums[i] * min_temp;
            max_temp = ((a > b) ? a : b) > nums[i] ? ((a > b) ? a : b) : nums[i];
            min_temp = ((a < b) ? a : b) < nums[i] ? ((a < b) ? a : b) : nums[i];
            max = (max > max_temp) ? max : max_temp;
        }
        return max;
    }
};