public class Solution {
    public int lengthOfLongestSubstring(String s) {
        if(s == null || s.isEmpty()) return 0;
        HashMap<Character,Integer> map = new HashMap<Character,Integer>();
        //i表示字符串终点，j表示下次查找的起点，只会越来越大
        int j = 0;
        int max = 0;
        for(int i = 0; i < s.length(); i++){
            if(map.containsKey(s.charAt(i))){
                j = Math.max(j,map.get(s.charAt(i)) + 1);
            }
            map.put(s.charAt(i),i);
            max = Math.max(max,i - j + 1);
        }
        return max;
    }
}