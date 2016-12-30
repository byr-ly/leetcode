class Solution {
public:
    int getMoneyAmount(int n) {
        vector<vector<int>> ans(n + 1,vector<int>(n + 1));
        return getResult(ans,1,n);
    }
    
    int getResult(vector<vector<int>>& ans,int l,int r){
        if(l >= r) return 0;
        if(ans[l][r]) return ans[l][r];
        int res = INT_MAX;
        for(int i = l; i < r; i++){
            int max = (getResult(ans,l,i - 1) > getResult(ans,i + 1,r)) ? getResult(ans,l,i - 1) : getResult(ans,i + 1,r);
            int temp = i + max;
            res = (res < temp) ? res : temp;
        }
        ans[l][r] = res;
        return res;
    }
};