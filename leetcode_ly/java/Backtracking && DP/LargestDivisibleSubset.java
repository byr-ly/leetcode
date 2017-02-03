public class Solution {
    public List<Integer> largestDivisibleSubset(int[] nums) {
        List<Integer> res = new ArrayList<Integer>();
        if(nums == null || nums.length == 0) return res;
        Arrays.sort(nums);
        int[] dp = new int[nums.length];
        Arrays.fill(dp,1);
        for(int i = 1; i < nums.length; i++){
            for(int j = 0; j < i; j++){
                if(nums[i] % nums[j] == 0) dp[i] = Math.max(dp[i],dp[j] + 1);
            }
        }
        
        int max = 0;
        int index = 0;
        for(int i = 0; i < dp.length; i++){
            if(dp[i] > max){
                max = dp[i];
                index = i;
            }
        }
        
        int count = dp[index];
        for(int i = index; i >= 0; i--){
            if(nums[index] % nums[i] == 0 && count == dp[i]){
                res.add(nums[i]);
                count--;
            }
        }
        return res;
    }
}