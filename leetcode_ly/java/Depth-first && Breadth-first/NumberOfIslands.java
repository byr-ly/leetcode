public class Solution {
    static int m;
    static int n;
    public int numIslands(char[][] grid) {
        if(grid == null || grid.length == 0) return 0;
        m = grid.length;
        n = grid[0].length;
        
        int count = 0;
        for(int i = 0; i < m; i++){
            for(int j = 0; j < n; j++){
                if(grid[i][j] == '1'){
                    dfs(grid,i,j);
                    count++;
                }
            }
        }
        return count;
    }
    
    public void dfs(char[][] grid,int row,int col){
        if(row < 0 || row >= m || col < 0 || col >= n || grid[row][col] != '1') return;
        //将同一个岛中的1全部变为0
        grid[row][col] = '0';
        
        dfs(grid,row - 1,col);
        dfs(grid,row + 1,col);
        dfs(grid,row,col - 1);
        dfs(grid,row,col + 1);
    }
}