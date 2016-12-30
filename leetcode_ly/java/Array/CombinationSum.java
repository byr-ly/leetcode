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
                //������Ϊi��Ϊ�˱����������ظ�
                dfs(res,ans,candidates,sum,target,i);
                ans.remove(ans.size() - 1);
                sum -= candidates[i];
            }
        }
        return;
    }
}