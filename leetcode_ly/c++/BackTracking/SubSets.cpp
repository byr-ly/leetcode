class Solution {
public:
    vector<vector<int>> subsets(vector<int>& nums) {
        vector<int> elements;
        vector<vector<int>> result;
        int k = nums.size();
        if(k == 0) return result;
        
        result.push_back(elements);
        for(int i = 1; i <= k; i++){
            getResult(nums,result,elements,0,i);
        }
        return result;
    }
    
    void getResult(vector<int>& candidates,vector<vector<int>>& result,vector<int>& elements,int start,int k){
        if(elements.size() == k){
            result.push_back(elements);
            return;
        }
        
        for(int i = start; i < candidates.size(); i++){
            elements.push_back(candidates[i]);
            getResult(candidates,result,elements,i+1,k);
            elements.pop_back();
        }
    }
};