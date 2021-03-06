public class Solution {
    // you need to treat n as an unsigned value
    public int hammingWeight(int n) {
    	//>>表示带符号右移，>>>表示不带符号右移
        int res = 0;
        while(n != 0){
            if((n & 1) == 1) res++;
            n = n >>> 1;
        }
        return res;
    }
}