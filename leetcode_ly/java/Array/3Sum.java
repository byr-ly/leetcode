public class Solution {
    public List<List<Integer>> threeSum(int[] nums) {
        List<List<Integer>> temp = new ArrayList<List<Integer>>();
        ArrayList<Integer> ans = new ArrayList<Integer>();
        if(nums.length < 3) return temp;
        Arrays.sort(nums);
        
        int lastNum = Integer.MAX_VALUE;
        for(int i = 0; i < nums.length - 1; i++){
            if(nums[i] == lastNum) continue;
            int j = i + 1;
            int k = nums.length - 1;
            while(j < k){
                if(nums[i] + nums[j] + nums[k] == 0){
                    ans.add(nums[i]);
                    ans.add(nums[j]);
                    ans.add(nums[k]);
                    temp.add(new ArrayList<Integer>(ans));
                    ans.clear();
                    j++;
                    k--;
                }
                else if(nums[i] + nums[j] + nums[k] < 0){
                    j++;
                }
                else k--;
            }
            lastNum = nums[i];
        }
        
        List<List<Integer>> res = new ArrayList<List<Integer>>();
        for(int i = 0; i < temp.size(); i++){
            if(!res.contains(temp.get(i))) res.add(temp.get(i));
        }
        return res;
    }
}