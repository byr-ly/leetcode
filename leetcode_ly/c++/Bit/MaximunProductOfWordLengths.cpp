class Solution {
public:
    int maxProduct(vector<string>& words) {
        int len = words.size();
        if(len == 0) return 0;
        vector<int> wordBits(len);
        
        for(int i = 0; i < len; i++){
            for(int j = 0; j < words[i].size(); j++){
                wordBits[i] |= (1 << (words[i][j] - 'a'));
            }
        }
        
        int max = 0;
        for(int i = 0; i < wordBits.size() - 1; i++){
            for(int j = i + 1; j < wordBits.size(); j++){
                if((wordBits[i] & wordBits[j]) == 0){
                    int res = words[i].size() * words[j].size();
                    max = (res > max) ? res : max;
                }
            }
        }
        return max;
    }
};