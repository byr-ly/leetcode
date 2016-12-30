class Solution {
public:
    int search(vector<int>& nums, int target) {
        if(nums.size() == 0) return -1;
        int result;
        int minIndex = findMin(nums);
        if(target < nums[minIndex]) return -1;
        if(minIndex == 0){
            result = binary_search(nums,0,nums.size() - 1,target);
            return result;
        }
        else if(target > nums[minIndex - 1]) return -1;
        else{
            if(target >= nums[0]){
                result = binary_search(nums,0,minIndex - 1,target);
                return result;
            }
            else{
                result = binary_search(nums,minIndex,nums.size() - 1,target);
                return result;
            }
        }
    }
    
    int findMin(vector<int>& nums){
        if(nums.size() == 1) return 0;
        int i;
        for(i = 1; i < nums.size(); i++){
            if(nums[i] < nums[i - 1]) return i;
        }
        return 0;
    }
    
    int binary_search(vector<int>& nums,int left,int right,int target){
        int low = left;
        int high = right;
        while(low <= high){
            int middle = low + (high - low) / 2;
            if(nums[middle] == target) return middle;
            else if(nums[middle] > target) high = middle - 1;
            else low = middle + 1;
        }
        return -1;
    }
};