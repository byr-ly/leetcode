public class Solution {
    // you need to treat n as an unsigned value
    public int hammingWeight(int n) {
    	//>>±íÊ¾´ø·ûºÅÓÒÒÆ£¬>>>±íÊ¾²»´ø·ûºÅÓÒÒÆ
        int res = 0;
        while(n != 0){
            if((n & 1) == 1) res++;
            n = n >>> 1;
        }
        return res;
    }
}