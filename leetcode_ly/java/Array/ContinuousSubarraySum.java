public class Solution {
    public boolean checkSubarraySum(int[] nums, int k) {
        if(nums == null || nums.length == 0) return false;
        HashMap<Integer,Integer> map = new HashMap<>();
        map.put(0,-1);
        int sum = 0;
        for(int i = 0 ; i < nums.length; i++){
            sum += nums[i];
            if(k != 0) sum %= k;
            if(map.containsKey(sum)){
                int len = i - map.get(sum);
                if(len == 1) continue;
                else return true;
            }
            else{
                map.put(sum,i);
            }
        }
        return false;
    }
}