public class Solution {
    public List<List<Integer>> permute(int[] nums) {
        List<List<Integer>> res = new ArrayList<List<Integer>>();
        if(nums == null || nums.length == 0) return res;
        List<Integer> ans = new ArrayList<Integer>();
        getResult(res,nums,ans);
        return res;
    }
    
    public void getResult(List<List<Integer>> res,int[] nums,List<Integer> ans){
        if(ans.size() == nums.length){
            res.add(new ArrayList<>(ans));
            return;
        }
        
        for(int i = 0; i < nums.length; i++){
            if(ans.contains(nums[i])) continue;
            ans.add(nums[i]);
            getResult(res,nums,ans);
            ans.remove(ans.size() - 1);
        }
    }
}