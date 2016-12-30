public class Solution {
    public boolean isSubsequence(String s, String t) {
        if(s.length() > t.length()) return false;
        if(s.isEmpty()) return true;
        int ls = 0;
        int lt = 0;
        while(lt < t.length()){
            if(t.charAt(lt) == s.charAt(ls)){
                ls++;
                if(ls == s.length()) return true;
            }
            lt++;
        }
        return false;
    }
}