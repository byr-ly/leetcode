public class Solution {
    public int distributeCandies(int[] candies) {
        if(candies == null || candies.length == 0) return 0;
        HashMap<Integer,Integer> map = new HashMap<>();
        for(int i : candies){
            map.put(i,map.getOrDefault(i,0) + 1);
        }
        return map.size() >= candies.length / 2 ? candies.length / 2 : map.size();
    }
}