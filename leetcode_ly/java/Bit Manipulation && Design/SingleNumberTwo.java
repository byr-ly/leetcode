public class Solution {
    public int singleNumber(int[] nums) {
        if(nums == null || nums.length == 0) return 0;
        int res = 0;
        for(int i = 0; i < 32; i++){
            int sum = 0;
            for(int j = 0; j < nums.length; j++){
                if(((nums[j] >>> i) & 1) == 1){
                    sum++;
                    sum = sum % 3;
                }
            }
            if(sum != 0){
                res |= (sum << i);
            }
        }
        return res;
    }
}