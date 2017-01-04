//用快速排序击败9%的人
public class Solution {
    public int findKthLargest(int[] nums, int k) {
        quickSort(nums,0,nums.length - 1);
        return nums[nums.length - k];
    }
    
    public void quickSort(int[] nums,int left,int right){
        if(left <= right){
            int low = left;
            int high = right;
            int target = nums[low];
            while(low < high){
                while(low < high && nums[high] >= target){
                    high--;
                }
                nums[low] = nums[high];
                while(low < high && nums[low] <= target){
                    low++;
                }
                nums[high] = nums[low];
            }
            nums[low] = target;
            quickSort(nums,left,low - 1);
            quickSort(nums,low + 1,right);
        }
    }
}

//用优先队列击败了41%的人
public class Solution {
    public int findKthLargest(int[] nums, int k) {
        PriorityQueue<Integer> q = new PriorityQueue<Integer>(
            new Comparator<Integer>(){
                @Override
                public int compare(Integer a,Integer b){
                    return b - a;
                }
            });
        for(int i = 0; i < nums.length; i++){
            q.add(nums[i]);
        }
        while(k != 1){
            q.poll();
            k--;
        }
        return Integer.valueOf(q.poll());
    }
}