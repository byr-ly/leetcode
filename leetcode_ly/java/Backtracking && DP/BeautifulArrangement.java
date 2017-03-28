public class Solution {
    public int res = 0;
    public int countArrangement(int N) {
        if(N <= 0) return 0;
        int[] nums = new int[N + 1];
        boolean[] visit = new boolean[N + 1];
        for(int i = 0; i <= N; i++){
            nums[i] = i;
        }
        ArrayList<Integer> ans = new ArrayList<>();
        dfs(nums,visit,ans);
        return res;
    }
    
    public void dfs(int[] nums,boolean[] visit,ArrayList<Integer> ans){
        if(ans.size() == nums.length - 1){
            res++;
            return;
        }
        
        for(int i = 1; i < nums.length; i++){
            if(visit[i] || ((ans.size() + 1) % nums[i] != 0 && nums[i] % (ans.size() + 1) != 0)) continue;
            ans.add(nums[i]);
            visit[i] = true;
            dfs(nums,visit,ans);
            visit[i] = false;
            ans.remove(ans.size() - 1);
        }
    }
}