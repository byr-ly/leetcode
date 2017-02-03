public class Solution {
    public int integerBreak(int n) {
        if(n == 2) return 1;
        if(n == 3) return 2;
        //4=2*2 5<2*3...则一定按2或3来分，而6=2+2+2=3+3 3*3>2*2*2
        int res = 1;
        while(n > 4){
            res *= 3;
            n -= 3;
        }
        res *= n;
        return res;
    }
}