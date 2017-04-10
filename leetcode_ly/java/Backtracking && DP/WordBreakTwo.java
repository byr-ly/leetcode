public class Solution {
    public List<String> wordBreak(String s, List<String> wordDict) {
        List<String> res = new ArrayList<>();
        if(wordDict == null || wordDict.size() == 0) return res;
        dfs(res,s,"",wordDict,0);
        return res;
    }
    
    public void dfs(List<String> res,String s,String ptr,List<String> wordDict,int start){
        if(start == s.length()){
            res.add(ptr.substring(0,ptr.length() - 1));
            return;
        }
        
        for(int i = start + 1; i <= s.length(); i++){
            String str = s.substring(start,i);
            if(wordDict.contains(str)){
                ptr = ptr + str + " ";
                dfs(res,s,ptr,wordDict,i);
                ptr = ptr.substring(0,ptr.length() - str.length() - 1);
            }
        }
    }
}