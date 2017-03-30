public class Solution {
    public int[] nextGreaterElements(int[] nums) {
        int[] res = new int[nums.length];
        Arrays.fill(res,-1);
        Stack<Integer> s = new Stack<>();
        for(int i = 0; i < nums.length * 2; i++){
            int num = nums[i % nums.length];
            while(!s.isEmpty() && nums[s.peek()] < num){
                res[s.pop()] = num;
            }
            if(i < nums.length) s.push(i);
        }
        return res;
    }
}