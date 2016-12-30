class Solution {
public:
    bool isVowels(char a){
        if(a == 'a' || a == 'e' || a == 'i' || a == 'o' || a == 'u' || a == 'A' || a == 'E' || a == 'I' || a == 'O' || a == 'U'){
            return true;
        }
        else{
            return false;
        }
    }

    string reverseVowels(string s) {
        int i = 0;
        int j = s.length() - 1;
        while(i < j){
            if(isVowels(s[i]) && isVowels(s[j])){
                char temp = s[i];
                s[i] = s[j];
                s[j] = temp;
                i++;
                j--;
            }
            else if(isVowels(s[i]) && !isVowels(s[j])){
                j--;
            }
            else if(!isVowels(s[i]) && isVowels(s[j])){
                i++;
            }
            else{
                i++;
                j--;
            }
        }
        return s;
    }
};