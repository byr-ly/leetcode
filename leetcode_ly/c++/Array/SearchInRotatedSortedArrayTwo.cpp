class Solution {
public:
    bool search(vector<int>& nums, int target) {
        if(nums.size() == 0) return false;
        int minIndex = findMin(nums);
        if(target < nums[minIndex]) return false;
        if(minIndex == 0){
            return binary_search(nums,0,nums.size() - 1,target);
        }
        else if(target > nums[minIndex - 1]) return false;
        else{
            if(target >= nums[0]){
                return binary_search(nums,0,minIndex - 1,target);
            }
            else{
                return binary_search(nums,minIndex,nums.size() - 1,target);
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
    
    bool binary_search(vector<int>& nums,int left,int right,int target){
        int low = left;
        int high = right;
        while(low <= high){
            int middle = low + (high - low) / 2;
            if(nums[middle] == target) return true;
            else if(nums[middle] > target) high = middle - 1;
            else low = middle + 1;
        }
        return false;
    }
    
};


    
    
