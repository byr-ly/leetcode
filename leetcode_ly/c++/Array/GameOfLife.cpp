class Solution {
public:
    void gameOfLife(vector<vector<int>>& board) {
        int m = board.size();
        int n = board[0].size();
        
        for(int i = 0; i < m; i++){
            for(int j = 0; j < n; j++){
                int num = getLiveNum(board,i,j);
                if(board[i][j] == 1){
                    if(num < 2 || num > 3) board[i][j] = 2;
                }
                else{
                    if(num == 3) board[i][j] = 3;
                }
            }
        }
        
        for(int i = 0; i < m; i++){
            for(int j = 0; j < n; j++){
                board[i][j] %= 2;
            }
        }
    }
    
    int getLiveNum(vector<vector<int>>& board,int i,int j){
        int row = i - 1;
        int col = j - 1;
        int count = 0;
        for(int p = row; p < row + 3; p++){
            for(int q = col; q < col + 3; q++){
                if(p >= 0 && p < board.size() && q >= 0 && q < board[0].size()){
                    if(board[p][q] == 1 || board[p][q] == 2) count++;
                }
            }
        }
        count = count - board[i][j];
        return count;//把自己也算进去了
    }
};