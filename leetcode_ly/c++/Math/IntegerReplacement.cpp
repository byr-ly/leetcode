class Solution {
public:
    int integerReplacement(int n) {
        if(n == INT_MAX) return 32;
        int count = 0;
        while(n != 1){
            if(n % 2){
                if(n == 3) n = 2;
                else{
                    if(countZero(n - 1) >= countZero(n + 1)) n = n - 1;
                    else n = n + 1;
                }
            }
            else{
                n /= 2;
            }
            count++;
        }
        return count;
    }
    
    int countZero(int n){
        int cnt = 0;
        while((n & 1) == 0){
            cnt++;
            n = n >> 1;
        }
        return cnt;
    }
};