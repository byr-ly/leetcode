class Solution {
public:
    vector<vector<int>> combine(int n, int k) {
        vector<int> candidates;
        vector<int> elements;
        vector<vector<int>> result;
        if(n < k || k == 0 || n == 0) return result;
        for(int i = 1; i <= n; i++){
            candidates.push_back(i);
        }
        getResult(candidates,result,elements,0,k);
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