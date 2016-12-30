class Solution {
public:
    vector<int> spiralOrder(vector<vector<int>>& matrix) {
        vector<int> result;
        if(matrix.size() == 0) return result;
        int rows = matrix.size();
        int cols = matrix[0].size();
        int x1 = 0;
        int y1 = 0;
        
        while(rows >= 1 && cols >= 1){
            int x2 = x1 + rows - 1;
            int y2 = y1 + cols - 1;
            
            for(int i = y1; i <= y2; i++){
                result.push_back(matrix[x1][i]);
            }
            
            for(int i = x1 + 1; i <= x2 - 1; i++){
                result.push_back(matrix[i][y2]);
            }
            
            if(rows > 1){
                for(int i = y2; i >= y1; i--){
                    result.push_back(matrix[x2][i]);   
                }
            }
            
            if(cols > 1){
                for(int i = x2 - 1; i >= x1 + 1; i--){
                    result.push_back(matrix[i][y1]);
                }
            }
            x1++;
            y1++;
            rows -= 2;
            cols -= 2;
        }
        return result;
    }
};