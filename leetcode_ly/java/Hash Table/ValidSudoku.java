public class Solution {
    public boolean isValidSudoku(char[][] board) {
        int m = board.length;
        int n = board[0].length;
        
        int[][] row = new int[9][9];
        int[][] col = new int[9][9];
        int[][] block = new int[9][9];
        
        for(int i = 0; i < m; i++){
            for(int j = 0; j < n; j++){
                if(board[i][j] != '.'){
                    int num = board[i][j] - '1';
                    if(row[i][num] == 1 || col[num][j] == 1 || block[i / 3 * 3 + j / 3][num] == 1) return false;
                    row[i][num] = 1;
                    col[num][j] = 1;
                    block[i / 3 * 3 + j / 3][num] = 1;
                }
            }
        }
        return true;
    }
}