public class Solution {
    public void solveSudoku(char[][] board) {
        if(board.length == 0 || board[0].length == 0) return;
        solve(board);
    }
    
    public boolean solve(char[][] board){
        for(int i = 0; i < board.length; i++){
            for(int j = 0; j < board[0].length; j++){
                if(board[i][j] == '.'){
                    for(char k = '1'; k <= '9'; k++){
                        if(isValid(board,i,j,k)){
                            board[i][j] = k;
                            
                            if(solve(board)) return true;
                            else board[i][j] = '.';
                        }
                    }
                    return false;
                }
            }
        }
        return true;
    }
    
    public boolean isValid(char[][] board,int row,int col,char c){
        for(int i = 0; i < 9; i++){
            if(board[row][i] != '.' && board[row][i] == c) return false;
            if(board[i][col] != '.' && board[i][col] == c) return false;
            if(board[row / 3 * 3 + i / 3][col / 3 * 3 + i % 3] != '.' && board[row / 3 * 3 + i / 3][col / 3 * 3 + i % 3] == c) return false;
        }
        return true;
    }
}