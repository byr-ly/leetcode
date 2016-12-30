public class Solution {
    public List<List<Integer>> combinationSum2(int[] candidates, int target) {
        List<List<Integer>> res = new ArrayList<List<Integer>>();
        ArrayList<Integer> ans = new ArrayList<Integer>();
        Arrays.sort(candidates);
        dfs(res,ans,candidates,0,0,target);
        return res;
    }
    
    public void dfs(List<List<Integer>> res,ArrayList<Integer> ans,int[] candidates,int sum,int start,int target){
        if(sum == target){
            res.add(new ArrayList<>(ans));
            return;
        }
        else if(sum > target) return;
        else{
            for(int i = start; i < candidates.length; i++){
                //后面这句为了避免解决方案重复
                if(i != start && candidates[i] == candidates[i - 1]) continue;
                sum += candidates[i];
                ans.add(candidates[i]);
                dfs(res,ans,candidates,sum,i + 1,target);
                ans.remove(ans.size() - 1);
                sum -= candidates[i];
            }
        }
    }
}