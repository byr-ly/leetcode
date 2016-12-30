public class Solution {
    public boolean canConstruct(String ransomNote, String magazine) {
        if(ransomNote.length() > magazine.length()) return false;
        int[] pos = new int[26];
        for(int i = 0; i < ransomNote.length(); i++){
            pos[ransomNote.charAt(i) - 'a']++;
        }
        
        for(int j = 0; j < magazine.length(); j++){
            if(pos[magazine.charAt(j) - 'a'] != 0) pos[magazine.charAt(j) - 'a']--;
        }
        
        for(int k = 0; k < 26; k++){
            if(pos[k] != 0) return false;
        }
        return true;
    }
}