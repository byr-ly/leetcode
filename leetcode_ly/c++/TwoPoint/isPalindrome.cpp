class Solution {
public:
    bool isPalindrome(string s) {
        int len = s.size();
        if(len <= 1) return true;
        string str = "";
        for(int k = 0; k < len; k++){
            if(s[k] >= '0' && s[k] <= '9'){
                str += s[k];
            }
            if(s[k] >= 'a' && s[k] <= 'z'){
                str += s[k];
            }
            if(s[k] >= 'A' && s[k] <= 'Z'){
                str += (s[k] + 32);
            }
        }
        int length = str.size();
        int i = 0; 
        int j = length - 1;
        while(i < j){
           if(str[i] != str[j]){
                return false;
            } 
            i++;
            j--;
        }
        return true;
    }
};