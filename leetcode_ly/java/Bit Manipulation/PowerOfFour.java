public class Solution {
    public boolean isPowerOfFour(int num) {
        if(num <= 0) return false;
        while(num != 1){
            if(num % 4 != 0) return false;
            num = num / 4;
        }
        return num == 1;
    }
}


//without loop
public class Solution {
    public boolean isPowerOfFour(int num) {
        //大于0
        //是偶数
        //0x55555555是16进制数，是01010101010101010101010101010101，排除掉是2的阶乘不是4的阶乘的数
        return num > 0 && (num & (num - 1)) == 0 && (num & 0x55555555) != 0;
    }
}