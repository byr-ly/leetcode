class Solution {
public:
    vector<int> searchRange(vector<int>& nums, int target) {
        vector<int> result;
        if(nums.size() == 0){
            result.push_back(-1);
            result.push_back(-1);
            return result;
        }
        int low = 0;
        int high = nums.size() - 1;
        while(low <= high){
            int middle = low + (high - low) / 2;
            if(nums[middle] == target){
                int i = 1;
                while(nums[middle - i] == target && i <= middle){
                    i++;
                }
                result.push_back(middle - i + 1);
                i = 1;
                while(nums[middle + i] == target && middle + i < nums.size()){
                    i++;
                }
                result.push_back(middle + i - 1);
                return result;
            }
            else if(nums[middle] > target) high = middle - 1;
            else low = middle + 1;
        }
        result.push_back(-1);
        result.push_back(-1);
        return result;
    }
};