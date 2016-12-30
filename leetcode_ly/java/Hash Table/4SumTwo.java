public class Solution {
    public int fourSumCount(int[] A, int[] B, int[] C, int[] D) {
        HashMap<Integer,Integer> map = new HashMap<Integer,Integer>();
        int res = 0;
        for(int i = 0; i < A.length; i++){
            for(int j = 0; j < B.length; j++){
                int sum = A[i] + B[j];
                if(map.containsKey(sum)) map.put(sum,map.get(sum) + 1);
                else map.put(sum,1);
            }
        }
        for(int i = 0; i < C.length; i++){
            for(int j = 0; j < D.length; j++){
                int cnt = C[i] + D[j];
                if(map.containsKey(-1 * cnt)) res += map.get(-1 * cnt);
            }
        }
        return res;
    }
}