class NumMatrix {
public:
    NumMatrix(vector<vector<int>> &matrix) {
    		//resize很重要，要不然会超时
        if (matrix.empty()) return;
        int n = matrix.size(), m = matrix[0].size();
        mat.resize(n, vector<int>(m));
        mat[0][0] = matrix[0][0];
        for(int i = 1; i < matrix.size(); i++){
            mat[i][0] = matrix[i][0] + mat[i - 1][0];
        }
        for(int i = 1; i < matrix[0].size(); i++){
            mat[0][i] = matrix[0][i] + mat[0][i - 1];
        }
        for(int i = 1; i < matrix.size(); i++){
            for(int j = 1; j < matrix[0].size(); j++){
                mat[i][j] = matrix[i][j] + mat[i - 1][j] + mat[i][j - 1] - mat[i - 1][j - 1];
            }
        }
    }

    int sumRegion(int row1, int col1, int row2, int col2) {
        if(row1 == 0 && col1 == 0){
            return mat[row2][col2];
        }
        else if(row1 == 0){
            return mat[row2][col2] - mat[row2][col1 - 1];
        }
        else if(col1 == 0){
            return mat[row2][col2] - mat[row1 - 1][col2];
        }
        else{
            return mat[row2][col2] - mat[row2][col1 - 1] - mat[row1 - 1][col2] + mat[row1 - 1][col1 - 1];
        }
    }
public:
    vector<vector<int>> mat;
};


// Your NumMatrix object will be instantiated and called as such:
// NumMatrix numMatrix(matrix);
// numMatrix.sumRegion(0, 1, 2, 3);
// numMatrix.sumRegion(1, 2, 3, 4);


//更简单的解法
class NumMatrix {
private:
    vector<vector<int>> acc;
public:
    NumMatrix(vector<vector<int>> &matrix) {
        if (matrix.empty()) return;
        int n = matrix.size(), m = matrix[0].size();
        acc.resize(n + 1, vector<int>(m + 1));
        for (int i = 0; i <= n; ++i) acc[i][0] = 0;
        for (int j = 0; j <= m; ++j) acc[0][j] = 0;
        for (int i = 1; i <= n; ++i) {
            for (int j = 1; j <= m; ++j) {
                acc[i][j] = acc[i][j-1] + acc[i-1][j] - acc[i-1][j-1] + matrix[i-1][j-1];
            }
        } 
    }

    int sumRegion(int row1, int col1, int row2, int col2) {
        return acc[row2+1][col2+1] - acc[row1][col2+1] - acc[row2+1][col1] + acc[row1][col1];
    }
};


// Your NumMatrix object will be instantiated and called as such:
// NumMatrix numMatrix(matrix);
// numMatrix.sumRegion(0, 1, 2, 3);
// numMatrix.sumRegion(1, 2, 3, 4);