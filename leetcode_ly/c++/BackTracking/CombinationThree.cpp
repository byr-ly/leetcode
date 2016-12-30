class Solution {
public:
    vector<vector<int>> combinationSum3(int k, int n) {
        vector<int> vec;
        vector<vector<int>> res;
        int a[] = {1,2,3,4,5,6,7,8,9};
        vector<int> candidates(a,a + 9);
        if(k == 0 || n == 0) return res;
        getResult(candidates,res,vec,n,k,0);
        return res;
    }
    
    void getResult(vector<int>& candidates,vector<vector<int>>& res,vector<int>& vec,int target,int k,int start){
        if(!target && !k){
            res.push_back(vec);
            return;
        }
        for(int i = start; i < candidates.size(); i++){
            if(candidates[i] > target || k == 0) break;
            vec.push_back(candidates[i]);
            getResult(candidates,res,vec,target - candidates[i],k - 1,i + 1);
            vec.pop_back();
        }
    }
};