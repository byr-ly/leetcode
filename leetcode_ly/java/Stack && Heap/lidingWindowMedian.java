public class Solution {
    PriorityQueue<Integer> min = new PriorityQueue<Integer>();
        PriorityQueue<Integer> max = new PriorityQueue<Integer>(
            new Comparator<Integer>(){
                @Override
                public int compare(Integer a,Integer b){
                    return b.compareTo(a);
                }
            }
        );
    public double[] medianSlidingWindow(int[] nums, int k) {
        if(nums == null || nums.length < k) return new double[0];
        double[] res = new double[nums.length - k + 1];
        int i = 0; 
        for(;i <= nums.length; i++){
            if(i >= k){
                res[i - k] = getMedian();
                remove(nums[i - k]);
            }
            if(i < nums.length){
                add(nums[i]);
            }
        }
        return res;
    }
    
    public void add(int num){
        max.add(num);
        min.add(max.poll());
        if(min.size() > max.size()){
            max.add(min.poll());
        }
    }
    
    public void remove(int num){
        if(num > getMedian()){
            min.remove(num);
            if(min.size() < max.size() - 1){
                min.add(max.poll());
            }
        }
        else{
            max.remove(num);
            if(max.size() < min.size()){
                max.add(min.poll());
            }
        }
    }
    
    public double getMedian(){
        if(min.size() == max.size()){
            return ((double)min.peek() + (double)max.peek()) / 2;
        }
        else return (double)max.peek();
    }
}