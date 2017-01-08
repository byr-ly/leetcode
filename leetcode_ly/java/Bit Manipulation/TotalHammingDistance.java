public class Solution {
    public int totalHammingDistance(int[] nums) {
        int sum = 0;
        int n = nums.length;
        for(int i = 0; i < 32; i++){
            int cnt = 0;
            for(int j = 0; j < n; j++){
                //在每一位上统计有cnt个1,n - cnt个0
                cnt += ((nums[j] >>> i) & 1);
            }
            sum += cnt * (n - cnt);
        }
        return sum;
    }
}