public class Solution {
    public boolean makesquare(int[] nums) {
        if(nums == null || nums.length < 4) return false;
        int sum = 0;
        for(int num : nums){
            sum += num;
        }
        if(sum % 4 != 0) return false;
        //将数组逆序排序以后能更快的搜索到结果
        Arrays.sort(nums);
        reverse(nums);
        return dfs(nums,new int[4],0,sum / 4);
    }
    
    public boolean dfs(int[] nums,int[] sum,int idx,int target){
        if(idx == nums.length && sum[0] == target && sum[1] == target && sum[2] == target){
            return true;
        }
        
        for(int i = 0; i < 4; i++){
            if(sum[i] + nums[idx] > target) continue;
            sum[i] += nums[idx];
            if(dfs(nums,sum,idx + 1,target)) return true;
            sum[i] -= nums[idx];
        }
        return false;
    }
    
    public void reverse(int[] nums){
        int i = 0;
        int j = nums.length - 1;
        while(i < j){
            int temp = nums[i];
            nums[i] = nums[j];
            nums[j] = temp;
            i++;
            j--;
        }
    }
}