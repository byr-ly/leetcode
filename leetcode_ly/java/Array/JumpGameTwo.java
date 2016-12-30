public class Solution {
    public int jump(int[] nums) {
        int n = nums.length;
        if(n == 0) return Integer.MAX_VALUE;
        int[] dp = new int[n];
        dp[0] = 0;
        for(int i = 1; i < n; i++){
            dp[i] = Integer.MAX_VALUE;
        }
        for(int i = 1; i < n; i++){
            for(int j = 0; j < i; j++){
                if(j + nums[j] >= i){
                    int temp = dp[j] + 1;
                    if(temp < dp[i]){
                        dp[i] = temp;
                        break;
                    }
                    //����dp��һ���������У���������������Լ��ٴ����ļ���
                    //dp[i] = (dp[i] < dp[j] + i) ? dp[i] : dp[j] + 1;
                }
            }
        }
        return dp[n - 1];
    }
}