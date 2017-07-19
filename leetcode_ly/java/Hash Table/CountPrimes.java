public class Solution {
    public int countPrimes(int n) {
        boolean[] f = new boolean[n];
        int count = 0;
        for(int i = 2; i < n; i++){
            if(!f[i]){
                count++;
                for(int j = 2; i * j < n; j++){
                    f[i * j] = true;
                }
            }
        }
        return count;
    }
}