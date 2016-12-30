class Solution {
public:
    string reverseString(string s) {
        int len = s.size();
        char a;
        for(int i = 0; i < len; i++){
            if(i < len - i - 1){
                a = s[i];
                s[i] = s[len - i - 1];
                s[len - i - 1] = a;
            }
        }
        return s;
    }
};