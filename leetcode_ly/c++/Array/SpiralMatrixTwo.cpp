class Solution {
public:
    vector<vector<int>> generateMatrix(int n) {
        vector<vector<int>> matrix(n,vector<int>(n));
        int x1 = 0;
        int y1 = 0;
        int rows = n;
        int cols = n;
        int cnt = 1;
        while(rows >= 1 && cols >= 1){
            int x2 = x1 + rows - 1;
            int y2 = y1 + cols - 1;
            
            for(int i = y1; i <= y2; i++){
                matrix[x1][i] = cnt;
                cnt++;
            }
            
            for(int i = x1 + 1; i <= x2 - 1; i++){
                matrix[i][y2] = cnt;
                cnt++;
            }
            
            if(rows > 1){
                for(int i = y2; i >= y1; i--){
                    matrix[x2][i] = cnt;
                    cnt++;
                }
            }
            
            if(cols > 1){
                for(int i = x2 - 1; i >= x1 + 1; i--){
                    matrix[i][y1] = cnt;
                    cnt++;
                }
            }
            x1++;
            y1++;
            rows -= 2;
            cols -= 2;
        }
        return matrix;
    }
};