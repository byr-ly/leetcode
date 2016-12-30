class Solution {
public:
    int lengthOfLIS(vector<int>& nums) {
        int len = nums.size();
        vector<int> list(len);
        
        int max = 0;
        for(int i = 0; i < len; i++){
            for(int j = 0; j < i; j++){
                if(nums[j] < nums[i]){
                    list[i] = (list[i] > list[j]) ? list[i] : list[j];
                }
            }
            list[i] += 1;
            max = (max > list[i]) ? max : list[i];
        }
        return max;
    }
};