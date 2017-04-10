public class Solution {
    public List<List<Integer>> findSubsequences(int[] nums) {
        HashSet<List<Integer>> set = new HashSet<List<Integer>>();
        List<Integer> ans = new ArrayList<>();
        dfs(nums,set,ans,0);
        List<List<Integer>> res = new ArrayList<List<Integer>>(set);
        return res;
    }
    
    public void dfs(int[] nums,HashSet<List<Integer>> set,List<Integer> ans,int start){
        if(ans.size() >= 2){
            set.add(new ArrayList<>(ans));
        }
        for(int i = start; i < nums.length; i++){
            if(ans.size() == 0 || ans.get(ans.size() - 1) <= nums[i]){
                ans.add(nums[i]);
                dfs(nums,set,ans,i + 1);
                ans.remove(ans.size() - 1);
            }
        }
    }
}