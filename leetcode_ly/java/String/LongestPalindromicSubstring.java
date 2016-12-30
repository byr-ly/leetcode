public class Solution {
    private int low = 0;
    private int max = 0;
    public String longestPalindrome(String s) {
        if(s == null || s.isEmpty()) return "";
        if(s.length() < 2) return s;
        
        for(int i = 0; i < s.length(); i++){
            dfs(s,i,i);//假设长度为奇数
            dfs(s,i,i + 1);//假设长度为偶数
        }
        return s.substring(low, low + max);
    }
    
    public void dfs(String s,int left,int right){
        while(left >= 0 && right < s.length() && s.charAt(left) == s.charAt(right)){
            left--;
            right++;
        }
        if(max < right - left - 1){
            low = left + 1;
            max = right - left - 1;
        }
    }
}