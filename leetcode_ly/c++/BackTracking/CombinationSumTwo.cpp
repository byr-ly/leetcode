class Solution {
public:
    vector<vector<int>> combinationSum2(vector<int>& candidates, int target) {
        vector<int> elements;
        vector<vector<int>> result;
        if(candidates.size() == 0) return result;
        quickSort(candidates,0,candidates.size() - 1);
        getResult(candidates,result,elements,target,0);
        return result;
    }
    
    void quickSort(vector<int>& candidates,int left,int right){
        if(left < right){
            int low = left;
            int high = right;
            int target = candidates[left];
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
            quickSort(candidates,left,low - 1);
            quickSort(candidates,low + 1,right);
        }
    }
    
    void getResult(vector<int>& candidates,vector<vector<int>>& result,vector<int>& elements,int target,int start){
        if(!target){
            result.push_back(elements);
            return;
        }
        
        for(int i = start; i < candidates.size(); i++){
            if(candidates[i] > target) break;
            if(i > start && candidates[i] == candidates[i - 1]) continue;
            elements.push_back(candidates[i]);
            getResult(candidates,result,elements,target - candidates[i],i+1);
            elements.pop_back();
        }
    }
};