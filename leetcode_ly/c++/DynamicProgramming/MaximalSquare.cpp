class Solution {
public:
    int maximalSquare(vector<vector<char>>& matrix) {
        if(matrix.size() == 0) return 0;
        int m = matrix.size();
        int n = matrix[0].size();
        vector<vector<int>> dp(m,vector<int>(n));
        int max = 0;
        //给第一列赋边长
        for(int i = 0; i < m; i++){
            dp[i][0] = matrix[i][0] - '0';
            max = (max > dp[i][0]) ? max : dp[i][0];
        }
        //给第一行赋边长
        for(int j = 0; j < n; j++){
            dp[0][j] = matrix[0][j] - '0';
            max = (max > dp[0][j]) ? max : dp[0][j];
        }
        for(int i = 1; i < m; i++){
            for(int j = 1; j < n; j++){
                int temp = (dp[i - 1][j - 1] < dp[i - 1][j]) ? dp[i - 1][j - 1] : dp[i - 1][j];
                int min = (temp < dp[i][j - 1]) ? temp : dp[i][j - 1];
                dp[i][j] = matrix[i][j] == '1' ? min + 1 : 0;
                max = (max > dp[i][j]) ? max : dp[i][j];
            }
        }
        return max * max;
    }
};