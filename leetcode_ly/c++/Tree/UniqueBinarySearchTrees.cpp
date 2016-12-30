class Solution {
public:
    int numTrees(int n) {
        vector<int> ret(n + 1,0);
        ret[0] = ret[1] = 1;
        for(int i = 2; i < n + 1; i++){
            for(int j = 0; j < i; j++){
                ret[i] = ret[i] + ret[j] * ret[i - 1 - j];
            }
        }
        return ret[n];
    }
};