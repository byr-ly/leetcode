class Solution {
public:
    bool searchMatrix(vector<vector<int>>& matrix, int target) {
        if(matrix.size() == 0) return false;
        int m = matrix.size();
        int n = matrix[0].size();
        int row;
        int i;
        if(target <= matrix[0][n - 1]) row = 0;
        else{
            for(i = 1; i < m; i++){
                if(target == matrix[i][0]) return true;
                else if(target > matrix[i][0]) continue;
                else{
                    row = i - 1;
                    break;
                }
            }   
        }
        if(i == m) row = m - 1;
        for(int j = 0; j < n; j++){
            if(matrix[row][j] == target) return true;
            else continue;
        }
        return false;
    }
};