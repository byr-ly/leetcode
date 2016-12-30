class Solution {
public:
    int singleNumber(vector<int>& nums) {
        int len = nums.size();
        int result = nums[0];
        for(int i = 1; i < len; i++){
            result = result^nums[i];
        }
        return result;
    }
};