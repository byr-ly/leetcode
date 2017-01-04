public class Solution {
    public int[] maxSlidingWindow(int[] nums, int k) {
        int[] res = new int[nums.length - k + 1];
        if(nums.length == 0) return new int[0];
        PriorityQueue<Integer> q = new PriorityQueue<Integer>(
            new Comparator<Integer>(){
                @Override
                public int compare(Integer a,Integer b){
                    return b - a;
                }
            });
            
        for(int i = 0; i < res.length; i++){
            for(int j = i; j < i + k; j++){
                q.add(nums[j]);
            }
            res[i] = q.poll().intValue();
            q.clear();
        }
        return res;
    }
}