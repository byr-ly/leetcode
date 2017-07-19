public class Solution {
    public int arrayNesting(int[] nums) {
        if(nums == null || nums.length == 0) return 0;
        int max = Integer.MIN_VALUE;
        for(int i = 0; i < nums.length; i++){
            int size = 0;
            for(int k = i; nums[k] >= 0; size++){
                int idx = nums[k];
                //把计算过的元素标记为-1，否则会超时
                nums[k] = -1;
                k = idx;
            }
            max = Math.max(max,size);
        }
        return max;
    }
}