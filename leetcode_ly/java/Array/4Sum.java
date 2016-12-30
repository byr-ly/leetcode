public class Solution {
    public List<List<Integer>> fourSum(int[] nums, int target) {
        List<List<Integer>> temp = new ArrayList<List<Integer>>();
        if(nums.length < 4) return temp;
        int lasti = Integer.MAX_VALUE;
        Arrays.sort(nums);
        for(int i = 0; i < nums.length - 2; i++){
            if(nums[i] == lasti) continue;
            for(int j = i + 1; j < nums.length - 1; j++){
                int m = j + 1;
                int n = nums.length - 1;
                while(m < n){
                    if(nums[i] + nums[j] + nums[m] + nums[n] == target){
                        ArrayList<Integer> ans = new ArrayList<Integer>();
                        ans.add(nums[i]);
                        ans.add(nums[j]);
                        ans.add(nums[m]);
                        ans.add(nums[n]);
                        temp.add(ans);
                        m++;
                        n--;
                    }
                    else if(nums[i] + nums[j] + nums[m] + nums[n] < target) m++;
                    else n--;
                }
            }
            lasti = nums[i];
        }
        
        List<List<Integer>> res = new ArrayList<List<Integer>>();
        for(int i = 0; i < temp.size(); i++){
            if(!res.contains(temp.get(i))) res.add(temp.get(i));
        }
        return res;
    }
}