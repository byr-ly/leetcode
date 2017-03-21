public class Solution {
    public List<List<Integer>> combinationSum(int[] candidates, int target) {
        List<List<Integer>> res = new ArrayList<List<Integer>>();
        ArrayList<Integer> ans = new ArrayList<Integer>();
        Arrays.sort(candidates);
        dfs(res,ans,candidates,0,target,0);
        return res;
    }
    
    public void dfs(List<List<Integer>> res,ArrayList<Integer> ans,int[] candidates,int sum,int target,int start){
        if(sum == target){
            res.add(new ArrayList<>(ans));
            return;
        }
        else if(sum > target) return;
        else{
            for(int i = start; i < candidates.length; i++){
                sum += candidates[i];
                ans.add(candidates[i]);
                //最后参数为i是为了避免解决方案重复
                dfs(res,ans,candidates,sum,target,i);
                ans.remove(ans.size() - 1);
                sum -= candidates[i];
            }
        }
        return;
    }
}

public class Solution {
    public int combinationSum4(int[] nums, int target) {
        if(nums == null || nums.length == 0) return 0;
        int[] dp = new int[target + 1];
        dp[0] = 1;
        for(int i = 1; i < dp.length; i++){
            for(int j = 0; j < nums.length; j++){
                if(i >= nums[j]){
                    dp[i] += dp[i - nums[j]];
                }   
            }
        }
        return dp[target];
    }
}