public class Solution {
    public List<List<Integer>> combine(int n, int k) {
        List<List<Integer>> res = new ArrayList<List<Integer>>();
        List<Integer> ans = new ArrayList<Integer>();
        getResult(res,ans,n,k,1);
        return res;
    }
    
    public void getResult(List<List<Integer>> res,List<Integer> ans,int n,int k,int start){
        if(ans.size() == k){
            res.add(new ArrayList<Integer>(ans));
            return;
        }
        
        for(int i = start; i <= n; i++){
            ans.add(i);
            getResult(res,ans,n,k,i + 1);
            ans.remove(ans.size() - 1);
        }
    }
}