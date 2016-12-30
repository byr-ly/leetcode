public class Solution {
    public List<Integer> findAnagrams(String s, String p) {
        ArrayList<Integer> list = new ArrayList<Integer>();
        for(int i = 0; i <= s.length() - p.length(); i++){
            if(p.contains(String.valueOf(s.charAt(i)))){
                String str = s.substring(i,i + p.length());
                if(helper(str,p)) list.add(i);
            }
        }
        return list;
    }
    
    public boolean helper(String s,String p){
        int[] pos = new int[26];
        for(int i = 0; i < s.length(); i++){
            pos[s.charAt(i) - 'a']++;
        }
        for(int i = 0; i < p.length(); i++){
            pos[p.charAt(i) - 'a']--;
            if(pos[p.charAt(i) - 'a'] < 0) return false;
        }
        return true;
    }
}