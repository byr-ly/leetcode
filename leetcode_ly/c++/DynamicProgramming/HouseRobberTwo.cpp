class Solution {
public:
    int rob(vector<int>& nums) {
        int n = nums.size();
        vector<int> maxNum(n,0);
        if(n == 0) return 0;
        if(n == 1) return nums[0];
        maxNum[0] = nums[0];
        maxNum[1] = (nums[0] > nums[1]) ? nums[0] : nums[1];
        for(int i = 2; i < n - 1; i++){
            maxNum[i] = (maxNum[i - 2] + nums[i] > maxNum[i - 1]) ? maxNum[i - 2] + nums[i] : maxNum[i - 1];
        } 
        int first = maxNum[n - 2];
        
        maxNum[0] = 0;
        maxNum[1] = nums[1];
        for(int i = 2; i < n; i++){
            maxNum[i] = (maxNum[i - 2] + nums[i] > maxNum[i - 1]) ? maxNum[i - 2] + nums[i] : maxNum[i - 1];
        }
        int second = maxNum[n - 1];
        return (first > second) ? first : second;
    }
};