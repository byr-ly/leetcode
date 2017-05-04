public class Solution {
    public int thirdMax(int[] nums) {
        Arrays.sort(nums);
        int count = 0;
        int i;
        for(i = nums.length - 1; i > 0; i--){
            if(nums[i] != nums[i - 1]) count++;
            if(count == 2) break;
        }
        return (count == 2) ? nums[i - 1] : nums[nums.length - 1];
    }
}

public class Solution {
    public int thirdMax(int[] nums) {
        //third
        Integer first = null;
        Integer second = null;
        Integer third = null;
        for(Integer n : nums){
            if(n.equals(first) || n.equals(second) || n.equals(third)) continue;
            if(third == null || n > third){
                first = second;
                second = third;
                third = n;
            }
            else if(second == null || n > second){
                first = second;
                second = n;
            }
            else if(first == null || n > first){
                first = n;
            }
        }
        return first == null ? third : first;
    }
}