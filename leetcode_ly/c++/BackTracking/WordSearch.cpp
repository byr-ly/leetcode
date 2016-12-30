class Solution {
public:
    bool exist(vector<vector<char>>& board, string word) {
        if(word.size() == 0) return false;
        int m = board.size();
        int n = board[0].size();
        //记录是否已经走过
        vector<vector<bool>> visited(m,vector<bool>(n));
        
        for(int i = 0; i < m; i++){
            for(int j = 0; j < n; j++){
                visited[i][j] = false;
            }
        }
        
        for(int i = 0; i < m; i++){
            for(int j = 0; j < n; j++){
                if(board[i][j] == word[0]){
                    visited[i][j] = true;
                    if(word.size() == 1 || search(board,word.substr(1),i,j,visited)) return true;
                    visited[i][j] = false;
                }
            }
        }
        return false;
    }
    
    bool search(vector<vector<char>>& board,string word,int i,int j,vector<vector<bool>>& visited){
        if(word.size() == 0) return true;
        
        //四个方向
        vector<vector<int>> direct(4,vector<int>(2));
        direct[0][0] = direct[2][1] = direct[1][0] = direct[3][1] = 0;
        direct[0][1] = direct[2][0] = 1;
        direct[3][0] = direct[1][1] = -1;
        for(int k = 0; k < direct.size(); k++){
            int ii = i + direct[k][0];
            int jj = j + direct[k][1];
            if(ii >= 0 && jj >= 0 && ii < board.size() && jj < board[0].size() && !visited[ii][jj] && board[ii][jj] == word[0]){
                visited[ii][jj] = true;
                if(word.size() == 1 || search(board,word.substr(1),ii,jj,visited)) return true;
                visited[ii][jj] = false;
            }
        }
        return false;
    }
};