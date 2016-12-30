class Solution {
public:
    string getPermutation(int n, int k) {
        string s = "";
        if(n == 0) return s;
        vector<char> candidates;
        int count = 1;
        for(int i = 1; i <= n; i++){
            candidates.push_back(i + '0');
            count *= i;
        }
        k--;
        for(int i = 0; i < n; i++){
            count = count / (n - i);
            int pos = k / count;
            s += candidates[pos];
            
            for(int j = pos; j < n - i - 1; j++){
                candidates[j] = candidates[j + 1];
            }
            k = k % count;
        }
        return s;
    }    
};