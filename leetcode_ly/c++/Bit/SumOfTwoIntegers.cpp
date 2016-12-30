class Solution {
public:
    int getSum(int a, int b) {
        vector<int> ans(32);
        int res = 0;
        int flag = 0;
        for(int i = 0; i < 32; i++){
            ans[i] = ans[i] + flag + ((a >> i) & 1)+ ((b >> i) & 1);
            if(ans[i] == 2){
                flag = 1;
                ans[i] = 0;
            }
            else if(ans[i] == 3){
                flag = 1;
                ans[i] = 1;
            }
            else flag = 0;
            res |= (ans[i] << i);
        }
        return res;
    }
};