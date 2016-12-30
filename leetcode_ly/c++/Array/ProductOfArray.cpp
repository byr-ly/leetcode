class Solution {
public:
    vector<int> productExceptSelf(vector<int>& nums) {
        int len = nums.size();
        vector<int> res(len,1);
        int left = 1;
        int right = 1;
        for(int i = 0; i < len; i++){
            res[i] = left;
            left *= nums[i];
        }
        for(int j = len - 1; j >= 0; j--){
            res[j] *= right;
            right *= nums[j];
        }
        return res;
    }
};