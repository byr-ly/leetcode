public class Solution {
    public List<List<Integer>> permuteUnique(int[] nums) {
        List<List<Integer>> res = new ArrayList<List<Integer>>();
        if(nums == null || nums.length == 0) return res;
        List<Integer> ans = new ArrayList<Integer>();
        Arrays.sort(nums);
        boolean [] used = new boolean[nums.length];
        getResult(res,nums,ans,used);
        return res;
    }
    
    public void getResult(List<List<Integer>> res,int[] nums,List<Integer> ans,boolean[] used){
        if(ans.size() == nums.length){
            res.add(new ArrayList<>(ans));
            return;
        }
        
        for(int i = 0; i < nums.length; i++){
            if(used[i] || (i > 0 && nums[i] == nums[i - 1] && !used[i - 1])) continue;
            used[i] = true;
            ans.add(nums[i]);
            getResult(res,nums,ans,used);
            ans.remove(ans.size() - 1);
            used[i] = false;
        }
    }
}