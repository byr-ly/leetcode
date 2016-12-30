public class Solution {
    public int findDuplicate(int[] nums) {
        int m = 0;
        int n = nums.length - 1;
        while(m <= n){
            int mid = m + (n - m) / 2;
            int cnt = 0;
            for(int i = 0; i < nums.length; i++){
                if(nums[i] <= mid) cnt++;
            }
            if(cnt > mid) n = mid - 1;
            else m = mid + 1;
        }
        return m;
    }
}