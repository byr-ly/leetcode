#include <string>

class Solution {
public:
    string convertToTitle(int n) {
        string s = "";
        while(n != 0){
            int res = n % 26;
            n = n / 26;
            if(res == 0){
                s  = s + 'Z';
                if(n == 1) break;
                else if(n < 28){
                    s += ('A' + n - 2);
                    break;
                }
            }
            else{
                s += ('A' + res - 1);
            }
        }
        int i,j;
        for(i = 0, j = s.size() - 1; i < j; i++, j--){
            char t = s[i];
            s[i] = s[j];
            s[j] = t;
        }
        return s;
    }
};