class Solution {
public:
    int maxProfit(vector<int>& prices) {
        int pre_sell = 0;
        int pre_buy = 0;
        int sell = 0;
        int buy = INT_MIN;
        for(int i = 0; i < prices.size(); i++){
            pre_buy = buy;
            buy = (pre_sell - prices[i] > pre_buy) ? (pre_sell - prices[i]) : pre_buy;
            pre_sell = sell;
            sell = (pre_buy + prices[i] > pre_sell) ? (pre_buy + prices[i]) : pre_sell;
        } 
        return sell;
    }
};