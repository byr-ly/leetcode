public class Solution {
    public int cnt = 0;
    public int totalNQueens(int n) {
        char[][] ans = new char[n][n];
        for(int i = 0; i < n; i++){
            for(int j = 0; j < n; j++){
                ans[i][j] = '.';
            }
        }
        dfs(ans,0);
        return cnt;
    }
    
    public void dfs(char[][] ans,int k){
        int n = ans.length;
        if(k == n){
            cnt++;
            return;
        }
        
        for(int i = 0; i < n; i++){
            if(isValid(ans,i,k)){
                ans[i][k] = 'Q';
                dfs(ans,k + 1);
                ans[i][k] = '.';
            }
        }
    }
    
    public boolean isValid(char[][] ans,int x,int y){
        for(int i = 0; i < ans.length; i++){
            for(int j = 0; j < ans[0].length; j++){
                if(ans[i][j] == 'Q' && (x + j == y + i || x + y == i + j || x == i)){
                    return false;
                }
            }
        }
        return true;
    }
}