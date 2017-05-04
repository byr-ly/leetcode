public class Solution {
    public boolean canCross(int[] stones) {
        if(stones == null || stones.length == 0) return false;
        //存每一块石头上能走的步数
        HashMap<Integer,HashSet<Integer>> map = new HashMap<>();
        for(int i = 0; i < stones.length; i++){
            map.put(stones[i],new HashSet<>());
        }
        map.get(0).add(1);
        for(int i = 0; i < stones.length - 1; i++){
            int stone = stones[i];
            if(map.containsKey(stone)){
                HashSet<Integer> set = map.get(stone);
                for(Integer step : set){
                    int reach = stone + step;
                    if(reach == stones[stones.length - 1]) return true;
                    if(map.containsKey(reach)){
                        HashSet<Integer> set1 = map.get(reach);
                        set1.add(step);
                        if(step - 1 > 0) set1.add(step - 1);
                        set1.add(step + 1);
                    }
                }
            }
        }
        return false;
    }
}