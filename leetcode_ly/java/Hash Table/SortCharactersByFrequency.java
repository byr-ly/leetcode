public class Solution {
    public String frequencySort(String s) {
        Map<Character,Integer> map = new HashMap<Character,Integer>();
        for(Character c : s.toCharArray()){
            if(map.containsKey(c)) map.put(c,map.get(c) + 1);
            else map.put(c,1);
        }
        
        PriorityQueue<Map.Entry<Character,Integer>> q = new PriorityQueue<>(
            new Comparator<Map.Entry<Character,Integer>>(){
                @Override
                public int compare(Map.Entry<Character,Integer> a,Map.Entry<Character,Integer> b){
                    return b.getValue() - a.getValue();
                }
            }
        );
        q.addAll(map.entrySet());
        StringBuffer str = new StringBuffer();
        while(!q.isEmpty()){
            Map.Entry e = q.poll();
            //注意下面e.getValue()是Integer类型，没有遇到算术运算是不会自动拆箱的
            for(int i = 0; i < (int)e.getValue(); i++){
                str.append(e.getKey());
            }
        }
        return str.toString();
    }
}