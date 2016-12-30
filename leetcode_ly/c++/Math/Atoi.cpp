class Solution {
public:
    int myAtoi(string str) {
        if(str == "") return 0;
        double num = 0;
        int flag = 1;
        int i = 0;
        while(i < str.size() && str[i] == ' '){
            i++;
        }
        if(i == str.size()) return 0;
        
        if(str[i] == '+'){
            flag = 1;
            i++;
        }
        else if(str[i] == '-'){
            flag = -1;
            i++;
        }
        
        while(i < str.size()){
            if(str[i] >= '0' && str[i] <= '9'){
                num = num * 10 + flag * (str[i] - '0'); 
                if(num > 2147483647 || num < -2147483648){
                    num = (num > 0) ? 2147483647 : -2147483648;
                }
                i++;
            }
            else{
                break;
            }
        }
        return (int)num;
    }
};