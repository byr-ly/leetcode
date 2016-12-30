class Solution {
public:
    int uniquePaths(int m, int n) {
        if(m == 0) return 0;
        vector<vector<int>> grid(m,vector<int>(n));
        for(int i = 0; i < m; i++){
            grid[i][0] = 1;    
        }
        for(int j = 1; j < n; j++){
            grid[0][j] = 1;
        }
        for(int i = 1; i < m; i++){
            for(int j = 1; j < n; j++){
                grid[i][j] = grid[i][j - 1] + grid[i - 1][j];
            }
        }
        return grid[m - 1][n - 1];
    }
};