class Solution {
public:
    int uniquePathsWithObstacles(vector<vector<int>>& obstacleGrid) {
        int m = obstacleGrid.size();
        int n = obstacleGrid[0].size();
        if(m == 0 || obstacleGrid[0][0] == 1) return 0;
        
        for(int i = 0; i < m; i++){
            for(int j = 0; j < n; j++){
                if(obstacleGrid[i][j] == 1) obstacleGrid[i][j] = -1;
            }
        }
        for(int i = 0; i < m; i++){
            if(i >= 1 && obstacleGrid[i - 1][0] == -1) break;
            if(obstacleGrid[i][0] == -1) continue;
            obstacleGrid[i][0] = 1;
        }
        for(int j = 1; j < n; j++){
            if(obstacleGrid[0][j - 1] == -1) break;
            if(obstacleGrid[0][j] == -1) continue;
            obstacleGrid[0][j] = 1;
        }
        for(int i = 1; i < m; i++){
            for(int j = 1; j < n; j++){
                if(obstacleGrid[i][j] == -1) continue;
                if(obstacleGrid[i][j - 1] == -1 && obstacleGrid[i - 1][j] != -1){
                    obstacleGrid[i][j] = obstacleGrid[i - 1][j];
                }
                else if(obstacleGrid[i][j - 1] != -1 && obstacleGrid[i - 1][j] == -1){
                    obstacleGrid[i][j] = obstacleGrid[i][j - 1];
                }
                else if(obstacleGrid[i][j - 1] != -1 && obstacleGrid[i - 1][j] != -1){
                    obstacleGrid[i][j] = obstacleGrid[i][j - 1] + obstacleGrid[i - 1][j];
                }
            }
        }
        if(obstacleGrid[m - 1][n - 1] == -1) return 0;
        return obstacleGrid[m - 1][n - 1];
    }
};