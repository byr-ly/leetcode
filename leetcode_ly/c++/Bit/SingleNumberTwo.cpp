class Solution {
public:
    int singleNumber(vector<int>& nums) {
        vector<int> ans(32);
        int res = 0;
        for(int i = 0; i < 32; i++){
            for(int j = 0; j < nums.size(); j++){
                ans[i] += (nums[j] >> i) & 1;
            }
            res |= (ans[i] % 3) << i;
        }
        return res;
    }
};