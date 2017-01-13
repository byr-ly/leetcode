public class Solution {
    public int numSquares(int n) {
        int[] res = new int[n + 1];
        res[0] = 0;
        for(int i = 1; i <= n; i++){
            int ans = Integer.MAX_VALUE;
            int j = 1;
            while(i - j * j >= 0){
                //res[n] = Min{ res[n - i * i] + 1 },  n - i * i >= 0 && i >= 1
                ans = Math.min(ans,res[i - j * j] + 1);
                j++;
            }
            res[i] = ans;
        }
        return res[n];
    }
}