public class Solution {
    static int[] dx = {-1,1,0,0};
    static int[] dy = {0,0,-1,1};
    public int longestIncreasingPath(int[][] matrix) {
        if(matrix == null || matrix.length == 0) return 0;
        int m = matrix.length;
        int n = matrix[0].length;
        //存以当前位置为起点的递增长度，memorize                 
        int[][] res = new int[m][n];
        
        int max = Integer.MIN_VALUE;
        for(int i = 0; i < m; i++){
            for(int j = 0; j < n; j++){
                int len = dfs(matrix,res,m,n,i,j);
                max = Math.max(max,len);
            }
        }
        return max;
    }
    
    public int dfs(int[][] matrix,int[][] res,int m,int n,int x,int y){
        //利用已知结果省去重复计算
        if(res[x][y] != 0) return res[x][y];
        int max = 1;
        for(int i = 0; i < 4; i++){
            int p = x + dx[i];
            int q = y + dy[i];
            if(p < 0 || p >= m || q < 0 || q >= n || matrix[p][q] <= matrix[x][y]) continue;
            else {
                int len = 1 + dfs(matrix,res,m,n,p,q);
                max = Math.max(max,len);
            }
        }
        res[x][y] = max;
        return max;
    }
}