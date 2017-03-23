public class Solution {
    public boolean isSubsequence(String s, String t) {
        if(s.length() > t.length() || t == null) return false;
        int j = 0;
        for(int i = 0; i < s.length(); i++){
            for(; j < t.length(); j++){
                if(s.charAt(i) == t.charAt(j)) break;
            }
            if(j == t.length()) return false;
            j++;
        }
        return true;
    }
}