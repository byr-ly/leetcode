public class Solution {
    public List<List<String>> groupAnagrams(String[] strs) {
        List<List<String>> res = new ArrayList<List<String>>();
        if(strs.length == 0 || strs == null) return res;
        
        HashMap<String,ArrayList<String>> map = new HashMap<String,ArrayList<String>>();
        for(int i = 0; i < strs.length; i++){
            char[] c = strs[i].toCharArray();
            Arrays.sort(c);
            if(map.containsKey(String.valueOf(c))){
                ArrayList<String> list = map.get(String.valueOf(c));
                list.add(strs[i]);
            }
            else{
                ArrayList<String> list = new ArrayList<String>();
                list.add(strs[i]);
                map.put(String.valueOf(c),list);
            }
        }
        return new ArrayList<List<String>>(map.values());
    }
}