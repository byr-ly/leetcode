public class Solution {
    public int numTrees(int n) {
        int[] res = new int[n + 1];
        res[0] = res[1] = 1;
        for(int i = 2; i <= n; i++){
            int temp = 0;
            for(int j = 0; j < i; j++){
                //确定顶点以后，左子树和右子树的种类相乘为一个顶点的所有情况
                temp = temp + res[j] * res[i - j - 1];
            }
            res[i] = temp;
        }
        return res[n];
    }
}