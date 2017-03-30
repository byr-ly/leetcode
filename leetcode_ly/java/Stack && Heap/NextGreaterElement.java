public class Solution {
    public int[] nextGreaterElement(int[] findNums, int[] nums) {
        int[] res = new int[findNums.length];
        Stack<Integer> s = new Stack<>();
        HashMap<Integer,Integer> map = new HashMap<>();
        for(int i = 0; i < nums.length; i++){
            while(!s.isEmpty() && s.peek() < nums[i]){
                map.put(s.pop(),nums[i]);
            }
            s.push(nums[i]);
        }
        for(int i = 0; i < res.length; i++){
            res[i] = map.getOrDefault(findNums[i],-1);
        }
        return res;
    }
}