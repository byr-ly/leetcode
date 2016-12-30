public class Solution {
    public char findTheDifference(String s, String t) {
        int[] res = new int[26];
        for(int i = 0; i < s.length(); i++){
            res[s.charAt(i) - 'a']++;
        }
        int j;
        for(j = 0; j < t.length(); j++){
            if(res[t.charAt(j) - 'a'] > 0) res[t.charAt(j) - 'a']--;
            else return t.charAt(j);
        }
        return t.charAt(j);
    }
}