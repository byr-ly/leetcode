public class Solution {
    public List<String> findRepeatedDnaSequences(String s) {
        List<String> list = new ArrayList<String>();
        HashMap<String,Integer> map = new HashMap<String,Integer>();
        for(int i = 0; i <= s.length() - 10; i++){
            String str = s.substring(i,i + 10);
            if(map.containsKey(str)){
                if(map.get(str) == 1) list.add(str);
                map.put(str,map.get(str) + 1);
            }
            else map.put(str,1);
        }
        return list;
    }
}