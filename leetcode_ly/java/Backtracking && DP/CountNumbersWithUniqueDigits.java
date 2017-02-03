public class Solution {
    public int countNumbersWithUniqueDigits(int n) {
        if(n == 0) return 1;
        int res = 10;
        int digit  = 9;
        int remain = 9;
        while(n > 1 && remain > 0){
            digit *= remain;
            res += digit;
            n--;
            remain--;
        }
        return res;
    }
}œ