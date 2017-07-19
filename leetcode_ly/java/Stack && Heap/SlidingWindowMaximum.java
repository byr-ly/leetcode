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


public class Solution {
    public int[] maxSlidingWindow(int[] nums, int k) {
        if(nums == null || nums.length == 0) return new int[0];
        PriorityQueue<Integer> q = new PriorityQueue<>(
            new Comparator<Integer>(){
                @Override
                public int compare(Integer a,Integer b){
                    return b - a;
                }
        });
        int[] res = new int[nums.length - k + 1];
        for(int i = 0; i < nums.length; i++){
            if(i >= k){
                q.remove(nums[i - k]);
            }
            q.add(nums[i]);
            if(q.size() == k) res[i + 1 - k] = q.peek();
        }
        return res;
    }
}