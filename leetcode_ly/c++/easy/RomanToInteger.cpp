class Solution {
public:
    int romanToInt(string s) {
        char cur = s[0];
        int i = 0;
        int result = 0;
        while(i < s.size()){
            switch(s[i]){
                case 'I':
                    result += 1;
                    break;
                case 'V':{
                    if(cur == 'I')
                        result += 3;
                    else
                        result += 5;
                    break;
                }
                case 'X':{
                    if(cur == 'I')
                        result += 8;
                    else
                        result += 10;
                    break;
                }
                case 'L':{
                    if(cur == 'X')
                        result += 30;
                    else
                        result += 50;
                    break;
                }
                case 'C':{
                    if(cur == 'X')
                        result += 80;
                    else
                        result += 100;
                    break;
                }
                case 'D':{
                    if(cur == 'C')
                        result += 300;
                    else
                        result += 500;
                    break;
                }
                case 'M':{
                    if(cur == 'C')
                        result += 800;
                    else
                        result += 1000;
                    break;
                }
                default:
                    break;
            }
            cur = s[i];
            i++;
        }
        return result;
    }
};