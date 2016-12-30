class Solution {
public:
    int countNumbersWithUniqueDigits(int n) {
        if(n == 0) return 1;
        int cnt = 0;
        for(int i = 1; i <= n; i++){
            cnt += get(i);
        }
        return cnt;
    }
    
    int get(int n){
        if(n == 1) return 10;
        int res = 9;
        for(int i = 2; i <= n; i++){
            res *= (11 - i);
        }
        return res;
    }
};


class Solution {
public:
    int countNumbersWithUniqueDigits(int n) {
        if(n == 0) return 1;
        if(n == 1) return 10;
        int cnt = 9;
        int res = 10;
        for(int i = 2; i <= n; i++){
            cnt *= (11 - i);
            res += cnt;
        }
        return res;
    }
};