public class Solution {
    public int threeSumClosest(int[] nums, int target) {
        if(nums.length < 3) return target;
        int res = Integer.MAX_VALUE;
        int sum = Math.abs(nums[0] + nums[1] + nums[2] - target);
        quickSort(nums,0,nums.length - 1);
        int lastNum = Integer.MAX_VALUE;
        for(int i = 0; i < nums.length - 1; i++){
            if(nums[i] == lastNum) continue;
            int j = i + 1;
            int k = nums.length - 1;
            while(j < k){
                if(nums[i] + nums[j] + nums[k] == target) return target;
                else if(nums[i] + nums[j] + nums[k] < target){
                    sum = (res > target - (nums[i] + nums[j] + nums[k])) ? nums[i] + nums[j] + nums[k] : sum;
                    res = Math.min(res,target - (nums[i] + nums[j] + nums[k]));
                    j++;
                }
                else{
                    sum = (res > nums[i] + nums[j] + nums[k] - target) ? nums[i] + nums[j] + nums[k] : sum;
                    res = Math.min(res,nums[i] + nums[j] + nums[k] - target);
                    k--;
                }
            }
            lastNum = nums[i];
        }
        return sum;
    }
    
    public void quickSort(int[] nums,int left,int right){
        if(left >= right) return;
        int low = left;
        int high = right;
        int target = nums[low];
        while(low < high){
            while(low < high && nums[high] >= target){
                high--;
            }
            nums[low] = nums[high];
            while(low < high && nums[low] <= target){
                low++;
            }
            nums[high] = nums[low];
        }
        nums[low] = target;
        quickSort(nums,left,low - 1);
        quickSort(nums,low + 1,right);
    } 
}