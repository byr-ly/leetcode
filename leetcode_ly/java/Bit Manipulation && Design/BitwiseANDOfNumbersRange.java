public class Solution {
    public int rangeBitwiseAnd(int m, int n) {
        int num = 0;
        //有几次不相等就说明后面需要补几个0
        while(m != n){
            m = m >>> 1;
            n = n >>> 1;
            num++;
        }
        return m << num;
    }
}