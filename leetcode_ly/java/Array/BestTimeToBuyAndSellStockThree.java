public class Solution {
    public int maxProfit(int[] prices) {
        int n = prices.length;
        if(n < 2) return 0;
        int[] preProfit = new int[n];
        int[] postProfit = new int[n];
        
        //�����i��֮ǰ����һ�λ�õ��������
        int curMin = prices[0];
        preProfit[0] = 0;
        for(int i = 1; i < n; i++){
            curMin = Math.min(curMin,prices[i]);
            preProfit[i] = Math.max(preProfit[i - 1],prices[i] - curMin);
        }
        
        //�����i��֮����һ�λ�õ��������
        int curMax = prices[n - 1];
        postProfit[n - 1] = 0;
        for(int i = n - 2; i >= 0; i--){
            curMax = Math.max(prices[i],curMax);
            postProfit[i] = Math.max(postProfit[i + 1],curMax - prices[i]);
        }
        
        int max = Integer.MIN_VALUE;
        for(int i = 0; i < n; i++){
            max = Math.max(max,preProfit[i] + postProfit[i]);
        }
        return max;
    }
}