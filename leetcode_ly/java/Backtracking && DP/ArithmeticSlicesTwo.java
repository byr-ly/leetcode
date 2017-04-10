public class Solution {
    public int numberOfArithmeticSlices(int[] A) {
        if(A == null || A.length <= 2) return 0;
        Map<Integer,Integer>[] map = new Map[A.length];
        int res = 0;
        for(int i = 0; i < A.length; i++){
            map[i] = new HashMap<>();
            for(int j = 0; j < i; j++){
                long diff = (long)A[i] - A[j];
                if(diff > Integer.MAX_VALUE || diff < Integer.MIN_VALUE) continue;
                int m = map[i].getOrDefault((int)diff,0);
                int n = map[j].getOrDefault((int)diff,0);
                res += n;
                map[i].put((int)diff,m + n + 1);
            }
        }
        return res;
    }
}