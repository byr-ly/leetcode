class Solution {
public:
    vector<vector<int>> combinationSum(vector<int>& candidates, int target) {
        vector<int> elements;
        vector<vector<int>> result;
        if(candidates.size() == 0) return result;
        int left = 0; 
        int right = candidates.size() - 1;
        quickSort(candidates,left,right);
        getResult(candidates,result,elements,target,0);
        return result;
    }
    
    void quickSort(vector<int>& candidates,int left,int right){
        if(left < right){
            int target = candidates[left];
            int low = left;
            int high = right;
            while(low < high){
                    while(low < high && candidates[high] >= target){
                        high--;
                    }
                    candidates[low] = candidates[high];
                    while(low < high && candidates[low] <= target){
                        low++;
                    }
                    candidates[high] = candidates[low];
            }
            candidates[low] = target;
            quickSort(candidates,low + 1,right);
            quickSort(candidates,left,low - 1);
        }
    }
    //start避免出现重复结果
    void getResult(vector<int>& candidates,vector<vector<int>>& result,vector<int> elements,int target,int start){
        if(!target){
            result.push_back(elements);
            return;
        }
        for(int i = start; i < candidates.size(); i++){
            if(candidates[i] > target) break;
            elements.push_back(candidates[i]);
            getResult(candidates,result,elements,target - candidates[i],i);
            elements.pop_back();
        }
    }
};