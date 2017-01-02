public class Solution {
    public int maxEnvelopes(int[][] envelopes) {
        if(envelopes.length == 0 || envelopes[0].length == 0) return 0;
        Arrays.sort(envelopes,new Comparator<int[]>(){
            public int compare(int[] a,int[] b){
                if(a[0] == b[0]){
                    return b[1] - a[1];
                }
                else{
                    return a[0] - b[0]; 
                }
            }
        });
        
        int[] dp = new int[envelopes.length];
        for(int i = 0; i < dp.length; i++) dp[i] = 1; 
        for(int i = 1; i < envelopes.length; i++){
            for(int j = 0; j < i; j++){
                if(envelopes[i][1] > envelopes[j][1]) dp[i] = Math.max(dp[i],dp[j] + 1);
            }
        }
        int res = 0;
        for(int k : dp){
            res = Math.max(k,res);
        }
        return res;
    }
}