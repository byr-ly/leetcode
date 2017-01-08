public class Solution {
    public int hammingDistance(int x, int y) {
        int cnt = 0;
        for(int i = 0; i < 32; i++){
            //==的优先级比位运算要高
            if(((x & 1) ^ (y & 1)) == 1) cnt++;
            x = x >>> 1;
            y = y >>> 1;
        }
        return cnt;
    }
}