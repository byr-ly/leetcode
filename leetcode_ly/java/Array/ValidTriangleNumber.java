public class Solution {
    public int triangleNumber(int[] nums) {
        if(nums == null || nums.length < 3) return 0;
        Arrays.sort(nums);
        int n = nums.length;
        int res = 0;
        for(int i = n - 1; i >= 2; i--){
            int l = 0;
            int r = i - 1;
            while(l < r){
                if(nums[l] + nums[r] > nums[i]){
                    res += (r - l);
                    r--;
                }
                else{
                    l++;
                }
            }
        }
        return res;
    }
}