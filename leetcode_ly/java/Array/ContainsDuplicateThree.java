public class Solution {
    public boolean containsNearbyAlmostDuplicate(int[] nums, int k, int t) {
        if(nums == null || nums.length == 0 || k < 0) return false;
        TreeSet<Long> set = new TreeSet<>();
        for(int i = 0; i < nums.length; i++){
            Long ceil = set.ceiling((long)nums[i]);
            Long floor = set.floor((long)nums[i]);
            if((ceil != null && ceil - nums[i] <= t) || (floor != null && nums[i] - floor <= t)) return true;
            set.add((long)nums[i]);
            if(i >= k){
                set.remove((long)nums[i - k]);
            }
        }
        return false;
    }
}