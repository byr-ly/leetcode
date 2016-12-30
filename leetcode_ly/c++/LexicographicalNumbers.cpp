class Solution {
public:
    vector<int> lexicalOrder(int n) {
        vector<int> ans;
        for(int i = 1; i <= 9; i++){
            dfs(i,n,ans);
        }
        return ans;
    }
    
    void dfs(int num,int n,vector<int>& ans){
        if(num > n) return;
        ans.push_back(num);
        for(int j = 0; j <= 9; j++){
            if(num * 10 + j <= n){
                dfs(num * 10 + j,n,ans);
            }
        }
        return;
    }
};