public class Solution {
    static int[] dx = new int[]{-1,0,0,1};
    static int[] dy = new int[]{0,1,-1,0}; 
    public List<int[]> pacificAtlantic(int[][] matrix) {
        List<int[]> res = new ArrayList<int[]>();
        if(matrix == null || matrix.length == 0) return res;
        int m = matrix.length;
        int n = matrix[0].length;
        boolean[][] p = new boolean[m][n];
        boolean[][] a = new boolean[m][n];
        
        //初始化两条边
        for(int j = 0; j < n; j++){
            p[0][j] = true;
            a[m - 1][j] = true;
        }
        
        for(int i = 0; i < m; i++){
            p[i][0] = true;
            a[i][n - 1] = true;
        }
        
        //判断该点能不能流入两个海
        for(int j = 0; j < n; j++){
            dfs(matrix,p,0,j);
            dfs(matrix,a,m - 1,j);
        }
        
        for(int i = 0; i < m; i++){
            dfs(matrix,p,i,0);
            dfs(matrix,a,i,n - 1);
        }
        
        for(int i = 0; i < m; i++){
            for(int j = 0; j < n; j++){
                if(p[i][j] && a[i][j]){
                    res.add(new int[]{i,j});
                }
            }
        }
        return res;
    }
    
    public void dfs(int[][] matrix,boolean[][] dp,int row,int col){
        dp[row][col] = true;
        int m = matrix.length;
        int n = matrix[0].length;
        for(int i = 0; i < 4; i++){
            int p = row + dx[i];
            int q = col + dy[i];
            if(p < m && p >= 0 && q < n && q >= 0){
                if(matrix[row][col] <= matrix[p][q] && dp[p][q] == false) dfs(matrix,dp,p,q);
            }
        }
    }
}