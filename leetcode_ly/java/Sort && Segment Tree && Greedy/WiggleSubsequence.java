public class Solution {
    public int wiggleMaxLength(int[] nums) {
        if(nums.length <= 1) return nums.length;
        int num = nums[0];
        int k = 1;
        while(k < nums.length){
            if(nums[k] == num) k++;
            else break;
        }
        if(k == nums.length) return 1;
        
        int res = 2;
        //继续用k是因为要跳过之前相等的元素
        boolean flag = (nums[k] > nums[k - 1]);
        //k++;
        while(++k < nums.length){
            if(flag && nums[k] < nums[k - 1]){
                flag = false;
                res++;
            }
            else if(!flag && nums[k] > nums[k - 1]){
                flag = true;
                res++;
            }
            //k++;
        }
        return res;
    }
}