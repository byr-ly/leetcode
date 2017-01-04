public class Solution {
    public String removeDuplicateLetters(String s) {
        Map<Character,Integer> map = new HashMap<Character,Integer>();
        for(int i = 0; i < s.length(); i++){
            map.put(s.charAt(i),i);
        }
        
        StringBuffer res = new StringBuffer();
        int len = map.size();
        int j = 0;
        int begin = 0;
        int end = findMin(map);
        while(j < len){
            char c = 'z';
            int index = end;
            for(int k = begin; k <= end; k++){
                if(map.containsKey(s.charAt(k)) && s.charAt(k) < c){
                    c = s.charAt(k);
                    index = k;
                }
            }
            res = res.append(c);
            j++;
            if(map.containsKey(c)) map.remove(c);
            begin = index + 1;
            end = findMin(map);
        }
        return res.toString();
    }
    
    public int findMin(Map<Character,Integer> map){
        if(map.isEmpty()) return -1;
        int min = Integer.MAX_VALUE;
        for(int val : map.values()){
            min = Math.min(min,val);
        }
        return min;
    }
}