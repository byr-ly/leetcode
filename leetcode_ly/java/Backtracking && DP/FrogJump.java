public class Solution {
    public boolean canCross(int[] stones) {
        if(stones == null || stones.length == 0) return false;
        HashMap<Integer,HashSet<Integer>> map = new HashMap<>();
        for(int i : stones){
            map.put(i,new HashSet<Integer>());
        }
        map.get(0).add(1);
        for(int i = 0; i < stones.length; i++){
            int loc = stones[i];
            if(map.containsKey(loc)){
                HashSet<Integer> set = map.get(loc);
                for(int step : set){
                    int newLoc = loc + step;
                    if(newLoc == stones[stones.length - 1]) return true;
                    if(map.containsKey(newLoc)){
                        HashSet<Integer> newSet = map.get(newLoc);
                        newSet.add(step);
                        if(step - 1 > 0) newSet.add(step - 1);
                        newSet.add(step + 1);
                    }
                }
            }
        }
        return false;
    }
}