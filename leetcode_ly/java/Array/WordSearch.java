public class Solution {
    public boolean exist(char[][] board, String word) {
        if(word.length() == 0) return false;
        int m = board.length;
        if(m == 0) return false;
        int n = board[0].length;
        
        boolean[][] visited = new boolean[m][n];
        for(int i = 0; i < m; i++){
            for(int j = 0; j < n; j++){
                visited[i][j] = false;
            }
        }
        
        for(int i = 0; i < m; i++){
            for(int j = 0; j < n; j++){
                if(board[i][j] == word.charAt(0)){
                    visited[i][j] = true;
                    if(word.length() == 1 || search(board,word.substring(1),i,j,visited)) return true;
                    visited[i][j] = false;
                }
            }
        }
        return false;
    }
    
    public boolean search(char[][] board,String word,int i,int j,boolean[][] visited){
        int[][] direction = new int[4][2];
        direction[0][0] = direction[1][0] = direction[2][1] = direction[3][1] = 0;
        direction[0][1] = direction[2][0] = 1;
        direction[1][1] = direction[3][0] = -1;
        for(int p = 0; p < 4; p++){
            int ii = i + direction[p][0];
            int jj = j + direction[p][1];
            if(ii >= 0 && ii < board.length && jj >= 0 && jj < board[0].length && !visited[ii][jj] && board[ii][jj] == word.charAt(0)){
                visited[ii][jj] = true;
                if(word.length() == 1 || search(board,word.substring(1),ii,jj,visited)) return true;
                visited[ii][jj] = false;
            }
        }
        return false;
    }
}