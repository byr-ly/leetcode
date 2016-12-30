public class Solution {
    public List<List<Integer>> combinationSum3(int k, int n) {
        int[] candidates = {1,2,3,4,5,6,7,8,9};
        List<List<Integer>> res = new ArrayList<List<Integer>>();
        ArrayList<Integer> ans = new ArrayList<Integer>();
        dfs(res,ans,candidates,0,0,k,n);
        return res;
    }
    
    public void dfs(List<List<Integer>> res,ArrayList<Integer> ans,int[] candidates,int sum,int start,int k,int n){
        if(sum == n && ans.size() == k){
            res.add(new ArrayList<>(ans));
            return;
        }
        else if(ans.size() > k || sum > n) return;
        else{
            for(int i = start; i < candidates.length; i++){
                sum += candidates[i];
                ans.add(candidates[i]);
                dfs(res,ans,candidates,sum,i + 1,k,n);
                ans.remove(ans.size() - 1);
                sum -= candidates[i];
            }
        }
    }
}