#include<math.h>

class Solution {
public:
    bool isPowerOfTwo(int n) {
        double result = log10(n) / log10(2);
        return (int) result == result;
    }
};

class Solution {
public:
    bool isPowerOfFour(int num) {
        if(num <= 0){
            return false;
        }
        else{
            while(num != 1){
                int x = num % 4;
                if(x != 0){
                    return false;
                }
                num = num / 4;
            }
            return (num == 1);
        }
    }
};