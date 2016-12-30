public class Solution {
    public List<String> summaryRanges(int[] nums) {
        ArrayList<String> list = new ArrayList<String>();
        if(nums.length == 0) return list;
        int i = 0;
        while(i < nums.length){
            String s = new String();
            s += nums[i];
            //用来判断首尾一样的情况
            int temp = nums[i];
            i++;
            while(i < nums.length && nums[i] - nums[i - 1] == 1){
                i++;
            }
            s = s + "->" + nums[i - 1];
            if(temp == nums[i - 1]){
                s = new String();
                s += temp;
            }
            list.add(s);
        }
        return list;
    }
}