class Solution {
public:
    int searchInsert(vector<int>& nums, int target) {
        if(nums.size() == 0) return 0;
        int low = 0; 
        int high = nums.size() - 1;
        while(low <= high){
            int middle = low + (high - low) / 2;
            if(nums[middle] == target) return middle;
            else if(nums[middle] > target) high = middle - 1;
            else low = middle + 1;
        }
        if(low == high && target > nums[low]) return low + 1;
        else if(low == high && target < nums[low]) return low;
        else return low;
    }
};