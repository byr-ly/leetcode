class Solution {
public:
    int maxProfit(vector<int>& prices) {
        int len = prices.size();
        if(len == 0 || len ==1){
            return 0;
        }
        else{
            int profit = 0;
            int min = prices[0];
            for(int i = 1; i < len; i ++){
                min = (min < prices[i]) ? min : prices[i];
                profit = (profit > (prices[i] - min)) ? profit : (prices[i] - min);
            }
            return profit;
        }
    }
};