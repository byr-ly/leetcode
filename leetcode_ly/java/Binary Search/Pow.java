public class Solution {
    public double myPow(double x, int n) {
        if(n == 0) return 1;
        if(n == -2147483648) return (double)1 / (x * myPow(x,2147483647));
        if(n < 0){
            n = -n;
            x = 1 / x;
        }
        //这样能减少栈帧层数，否则会栈溢出
        return (n % 2 == 0) ? myPow(x * x,n / 2) : x * myPow(x * x,n / 2); 
    }
}