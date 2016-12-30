public class Solution {
    public List<List<Integer>> subsetsWithDup(int[] nums) {
        List<List<Integer>> result = new ArrayList<List<Integer>>();
        List<Integer> element = new ArrayList<Integer>();
        int k = nums.length;
        if(k == 0) return result;
        result.add(element);
        Arrays.sort(nums);
        
        for(int i = 1; i <= k; i++){
            getResult(nums,result,element,0,i);
        }
        return result;
    }
    
    void getResult(int[] nums,List<List<Integer>> result,List<Integer> element,int start,int k){
        if(element.size() == k){
            result.add(new ArrayList<Integer>(element));
            return;
        }
        
        for(int i = start;i < nums.length; i++){
            if(i != start && nums[i] == nums[i - 1]) continue;
            else{
                element.add(nums[i]);
                getResult(nums,result,element,i+1,k);
                element.remove(element.size() - 1);
            }
        }
    }
}