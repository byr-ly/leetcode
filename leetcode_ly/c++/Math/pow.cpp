class Solution {
public:
    double myPow(double x, int n) {
        if(n == 0 || x == 1){
            return 1;
        }
        if(n == -2147483648){
            return (double) 1 / (x * myPow(x,2147483647));
        }
        if(n < 0){
            return (double) 1 / myPow(x,(0 - n));
        }
        if(n % 2 == 0){
            return myPow(x * x,n / 2);
        }
        return x * myPow(x,n - 1);
    }
};