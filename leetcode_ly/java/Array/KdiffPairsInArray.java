public class Solution {
    public int findPairs(int[] nums, int k) {
        if(nums == null || nums.length == 0 || k < 0) return 0;
        HashMap<Integer,Integer> map = new HashMap<>();
        for(int i = 0; i < nums.length; i++){
            map.put(nums[i],map.getOrDefault(nums[i],0) + 1);
        }
        int res = 0;
        for(Map.Entry<Integer,Integer> entry : map.entrySet()){
            if(k == 0){
                if(entry.getValue() > 1) res++;
            }
            else{
                if(map.containsKey(entry.getKey() + k)) res++;
            }
        }
        return res;
    }
}