class Solution {
public:
    int nthSuperUglyNumber(int n, vector<int>& primes) {
        int len = primes.size();
        //每个质数被乘了多少次
        vector<int> index(len,0);
        vector<int> res;
        res.push_back(1);
        for(int i = 1; i < n; i++){
            int num = INT_MAX;
            for(int j = 0; j < len; j++){
                num = (num < primes[j] * res[index[j]]) ? num : primes[j] * res[index[j]];
            }
            res.push_back(num);
            for(int k = 0; k < len; k++){
                if(res[i] % primes[k] == 0) index[k]++;
            }
        }
        return res[n - 1];
    }
};