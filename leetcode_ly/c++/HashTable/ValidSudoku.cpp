class Solution {
public:
    bool isValidSudoku(vector<vector<char>>& board) {
        vector<int> temp(9,1);
        vector<vector<int>> row(9,temp);
        vector<vector<int>> col(9,temp);
        vector<vector<int>> block(9,temp);
        
        for(int i = 0; i < 9; i++){
            for(int j = 0; j < 9; j++){
                if(board[i][j] == '.'){
                    continue;
                }
                int k = board[i][j] - '1';
                if(row[i][k] && col[j][k] && block[i / 3 * 3 + j / 3][k]){
                    row[i][k] = col[j][k] = block[i / 3 * 3 + j / 3][k] = 0;
                }
                else{
                    return false;
                }
            }
        }
        return true;
    }
};