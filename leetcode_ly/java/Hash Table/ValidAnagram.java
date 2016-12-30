public class Solution {
    public boolean isAnagram(String s, String t) {
        if(s.length() != t.length()) return false;
        char[] ps = s.toCharArray();
        char[] pt = t.toCharArray();
        Arrays.sort(ps);
        Arrays.sort(pt);
        for(int i = 0; i < ps.length; i++){
            if(ps[i] != pt[i]) return false;
        }
        return true;
    }
}