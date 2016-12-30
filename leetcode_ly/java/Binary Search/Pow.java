public class Solution {
    public double myPow(double x, int n) {
        if(n == 0) return 1;
        if(n == -2147483648) return (double)1 / (x * myPow(x,2147483647));
        if(n < 0){
            n = -n;
            x = 1 / x;
        }
        //�����ܼ���ջ֡�����������ջ���
        return (n % 2 == 0) ? myPow(x * x,n / 2) : x * myPow(x * x,n / 2); 
    }
}