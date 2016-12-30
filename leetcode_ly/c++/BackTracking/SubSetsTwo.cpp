class Solution {
public:
    vector<vector<int>> subsetsWithDup(vector<int>& nums) {
        vector<int> elements;
        vector<vector<int>> result;
        if(nums.size() == 0) return result;
        
        quickSort(nums,0,nums.size() - 1);
        getResult(nums,result,elements,0);
        return result;
    }
      
    void quickSort(vector<int>& nums,int left,int right){
        if(left < right){
            int low = left;
            int high = right;
            int target = nums[low];
            while(low < high){
                while(low < high && nums[high] >= target){
                    high--;
                }
                nums[low] = nums[high];
                while(low < high && nums[low] <= target){
                    low++;
                }
                nums[high] = nums[low];
            }
            nums[low] = target;
            quickSort(nums,left,low - 1);
            quickSort(nums,low + 1,right);
        }
    }
    
    void getResult(vector<int>& candidates,vector<vector<int>>& result,vector<int>& elements,int start){
        result.push_back(elements);
        
        for(int i = start; i < candidates.size(); i++){
            if(i != start && candidates[i] == candidates[i - 1]) continue;
            elements.push_back(candidates[i]);
            getResult(candidates,result,elements,i+1);
            elements.pop_back();
        }
    }
};