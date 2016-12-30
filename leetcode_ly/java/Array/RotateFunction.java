public class Solution {
    public int maxRotateFunction(int[] A) {
        int len = A.length;
        if(len <= 1) return 0;
        int sum = 0;
        int count = 0;
        for(int i = 0; i < len; i++){
            sum += A[i];
            count += i * A[i];
        }
        
        int max = count;
        for(int i = len - 1; i > 0; i--){
            count = count - (len - 1) * A[i] + (sum - A[i]);
            max = (max > count) ? max : count;
        }
        return max;
    }
}