class Solution {
public:
    int trailingZeroes(int n) {
        if(n < 5) return 0;
        int cnt = 0;
        while(n / 5 != 0){
            cnt += n / 5;
            n = n / 5;
        }
        return cnt;
    }
};