class Solution {
public:
    int titleToNumber(string s) {
        int result = 0;
        for(int i = 0 ; i < s.length(); i++){
            result  = result + (s[i] - 'A' + 1) * pow(26,(s.length() - i - 1));
        }
        return result;
    }
};