public class Solution {
    public int leastBricks(List<List<Integer>> wall) {
        if(wall == null || wall.size() == 0) return 0;
        List<Integer> list = new ArrayList<>();
        for(int i = 0; i < wall.size(); i++){
            List<Integer> ans = wall.get(i);
            int sum = 0;
            for(int j = 0; j < ans.size() - 1; j++){
                sum += ans.get(j);
                list.add(sum);
            }
        }
        HashMap<Integer,Integer> map = new HashMap<>();
        for(int i : list){
            map.put(i,map.getOrDefault(i,0) + 1);
        }
        int max = 0;
        for(Integer val : map.values()){
            max = Math.max(max,val);
        }
        return wall.size() - max;
    }
}