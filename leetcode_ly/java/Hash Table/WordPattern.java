public class Solution {
    public boolean wordPattern(String pattern, String str) {
        String[] words = str.split("\\s+");
        if(pattern.length() != words.length) return false;
        
        HashMap<Character,String> map = new HashMap<Character,String>();
        for(int i = 0; i < words.length; i++){
            if(map.containsKey(pattern.charAt(i))){
                if(map.get(pattern.charAt(i)).equals(words[i])) continue;
                else return false;
            }
            else{
                if(map.containsValue(words[i])) return false;
                map.put(pattern.charAt(i),words[i]);
            }
        }
        return true;
    }
}