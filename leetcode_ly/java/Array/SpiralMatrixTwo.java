public class Solution {
    public int[][] generateMatrix(int n) {
        int[][] res = new int[n][n];
        int cnt = 1;
        int row = n;
        int col = n;
        int x = 0;
        int y = 0;
        while(row > 0 && col > 0){
            int xx = x + row - 1;
            int yy = y + col - 1;
            for(int i = y; i <= yy; i++){
                res[x][i] = cnt;
                cnt++;
            }
            for(int i = x + 1; i < xx; i++){
                res[i][yy] = cnt;
                cnt++;
            }
            if(row > 1){
                for(int i = yy; i >= y; i--){
                    res[xx][i] = cnt;
                    cnt++;
                }
            }
            if(col > 1){
                for(int i = xx - 1; i > x; i--){
                    res[i][y] = cnt;
                    cnt++;
                }
            }
            row -= 2;
            col -= 2;
            x++;
            y++;
        }
        return res;
    }
}