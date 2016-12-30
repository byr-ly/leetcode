public class Solution {
    public int maxProfit(int[] prices) {
        if(prices.length == 0) return 0;
        int profit = 0;
        int min_buy = prices[0];
        for(int i = 1; i < prices.length; i++){
            min_buy = Math.min(min_buy,prices[i]);
            profit = Math.max(profit,(prices[i] - min_buy));
        }
        return profit;
    }
}