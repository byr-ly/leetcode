class Solution {
public:
    int rob(vector<int>& nums) {
        int len = nums.size();
        if(len == 0) return 0;
        if(len == 1) return nums[0];
        if(len == 2) {
            int result = (nums[0] > nums[1]) ? nums[0] : nums[1];
        }
        int max = nums[0];
        maxNum.push_back(max);
        int i,j;
        for(i = 1,j = 2; j < len; i++,j++){
            max = (max > nums[i]) ? max : nums[i];
            maxNum.push_back(max);
            nums[j] = nums[j] + maxNum[j - 2];
        }
        int res = (nums[len - 1] > nums[len - 2]) ? nums[len - 1] : nums[len - 2];
        return res;
    }
    
    private:
        vector<int> maxNum;
};