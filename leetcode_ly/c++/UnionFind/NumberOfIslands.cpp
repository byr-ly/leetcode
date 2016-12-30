class Solution {
public:
    int numIslands(vector<vector<char>>& grid) {
        int row = grid.size();
        if(row == 0) return 0;
        int col = grid[0].size();
        int ans = 0;
        for(int i = 0; i < row; i++){
            for(int j = 0; j < col; j++){
                if(grid[i][j] == '1'){
                    dfs(grid,i,j);
                    ans++;
                }
            }
        }
        return ans;
    }
    
    void dfs(vector<vector<char>>& grid,int i,int j){
        int m = grid.size();
        int n = grid[0].size();
        if(i < 0 || j < 0 || i >= m || j >= n){
            return;
        }
        
        if(grid[i][j] == '1'){
            grid[i][j] = '2';
            dfs(grid,i - 1,j);
            dfs(grid,i + 1,j);
            dfs(grid,i,j - 1);
            dfs(grid,i,j + 1);
        }
    }
};