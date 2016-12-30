public class Solution {
    public int majorityElement(int[] nums) {
        HashMap<Integer,Integer> map = new HashMap<>();
        for(int i = 0; i < nums.length; i++){
            if(map.containsKey(nums[i])){
                int num = map.get(nums[i]);
                num++;
                if(num > nums.length / 2) return nums[i];
                map.put(nums[i],num);
            }
            else{
                if(1 > nums.length / 2) return nums[i];
                map.put(nums[i],1);
            }
        }
        return 0;
    }
}