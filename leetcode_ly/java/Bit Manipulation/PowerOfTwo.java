public class Solution {
    public boolean isPowerOfTwo(int n) {
    	//<=0的数不算2的阶乘
        if(n <= 0) return false;
        if(n == 1) return true;
        while(n != 1){
            if(n % 2 == 1) return false;
            n = n / 2;
        }
        return n == 1;
    }
}


//2的阶乘用Bit表示1的个数只有1个，这种方式还不如上面的高效
public class Solution {
    public boolean isPowerOfTwo(int n) {
        if(n <= 0) return false;
        int count = 0;
        while(n != 0){
            n = n & (n - 1);
            count++;
        }
        return count == 1;
    }
}