public class Solution {
    public void gameOfLife(int[][] board) {
        int m = board.length;
        if(m == 0) return;
        int n = board[0].length;
        
        for(int i = 0; i < m; i++){
            for(int j = 0; j < n; j++){
                if(board[i][j] == 1){
                    if(getNum(i,j,board) < 2) board[i][j] = 3;
                    else if(getNum(i,j,board) > 3) board[i][j] = 2;
                    else board[i][j] = 4;
                }
                else if(board[i][j] == 0){
                    if(getNum(i,j,board) == 3) board[i][j] = 5;
                }
            }
        }
        
        for(int i = 0; i < m; i++){
            for(int j = 0; j < n; j++){
                if(board[i][j] == 4 || board[i][j] == 5) board[i][j] = 1;
                else board[i][j] = 0;
            }
        }
        return;
    }
    
    public int getNum(int i,int j,int[][] board){
        int m = board.length;
        int n = board[0].length;
        int count = 0;
        for(int p = i - 1; p < i + 2; p++){
            for(int q = j - 1; q < j + 2; q++){
                if(p >= 0 && p < m && q >= 0 && q < n){
                    if(p == i && q == j) continue;
                    else{
                        if(board[p][q] == 1 || board[p][q] == 2 || board[p][q] == 3 || board[p][q] == 4){
                            count++;
                        }
                    }
                }
            }
        }
        return count;
    }
}