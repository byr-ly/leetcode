public class Solution {
    public int findMinMoves(int[] machines) {
        if(machines == null || machines.length == 0) return -1;
        int sum = 0;
        for(int i : machines){
            sum += i;
        }
        if(sum % machines.length != 0) return -1;
        int target = sum / machines.length;
        int[] dp = new int[machines.length];
        for(int i = 0; i < dp.length; i++){
            dp[i] = machines[i] - target;
        }
        int res = 0;
        int cnt = 0;
        for(int i : dp){
            cnt += i;
            res = Math.max(Math.max(res,Math.abs(cnt)),i);
        }
        return res;
    }
}