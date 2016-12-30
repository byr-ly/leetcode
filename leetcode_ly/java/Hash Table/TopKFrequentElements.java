public class Solution {
    public List<Integer> topKFrequent(int[] nums, int k) {
        List<Integer> list = new ArrayList<Integer>();
        HashMap<Integer,Integer> map = new HashMap<Integer,Integer>();
        for(int i : nums){
            if(map.containsKey(i)) map.put(i,map.get(i) + 1);
            else map.put(i,1);
        }
        
        PriorityQueue<Map.Entry<Integer,Integer>> q = new PriorityQueue<>(
            new Comparator<Map.Entry<Integer,Integer>>(){
                @Override
                public int compare(Map.Entry<Integer,Integer> a,Map.Entry<Integer,Integer> b){
                    return b.getValue() - a.getValue();
                }
            }    
        );
        q.addAll(map.entrySet());
        while(!q.isEmpty() && k != 0){
            Map.Entry e = q.poll();
            list.add((int)e.getKey());
            k--;
        }
        return list;
    }
}