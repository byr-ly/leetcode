public class Solution {
    public List<List<String>> partition(String s) {
        List<List<String>> res = new ArrayList<List<String>>();
        List<String> ans = new ArrayList<String>();
        getResult(res,ans,s,0);
        return res;
    }
    
    public void getResult(List<List<String>> res,List<String> ans,String s,int start){
        if(start == s.length()){
            res.add(new ArrayList<String>(ans));
            return;
        }
        
        for(int i = start; i < s.length(); i++){
            if(isPalindrome(s,start,i)){
                ans.add(s.substring(start,i + 1));
                getResult(res,ans,s,i + 1);
                ans.remove(ans.size() - 1);
            }
        }
    }
    
    public boolean isPalindrome(String s,int low,int high){
        while(low <= high){
            if(s.charAt(low) != s.charAt(high)){
                return false;
            }
            low++;
            high--;
        }
        return true;
    }
}