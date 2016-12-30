public class Solution {
    public String minWindow(String s, String t) {
        HashMap<Character,Integer> map = new HashMap<Character,Integer>();
        for(int i = 0; i < t.length(); i++){
            if(map.containsKey(t.charAt(i))){
                map.put(t.charAt(i),map.get(t.charAt(i)) + 1);
            }
            else{
                map.put(t.charAt(i),1);
            }
        }
        
        int left = 0;
        int minLeft = 0;
        int minLen = Integer.MAX_VALUE;
        int count = 0;
        //map中的value值大于0说明还有字符没找到，小于等于0说明窗口已经包含
        for(int right = 0; right < s.length(); right++){
            if(map.containsKey(s.charAt(right))){
                map.put(s.charAt(right),map.get(s.charAt(right)) - 1);
                if(map.get(s.charAt(right)) >= 0) count++;
            }
            
            while(count == t.length()){
                if(minLen > right - left + 1){
                    minLen = right - left + 1;
                    minLeft = left;
                }
                if(map.containsKey(s.charAt(left))){
                    map.put(s.charAt(left),map.get(s.charAt(left)) + 1);
                    if(map.get(s.charAt(left)) > 0) count--;
                }
                left++;
            }
        }
        return (minLen == Integer.MAX_VALUE) ? "" : s.substring(minLeft,minLeft + minLen);
    }
}