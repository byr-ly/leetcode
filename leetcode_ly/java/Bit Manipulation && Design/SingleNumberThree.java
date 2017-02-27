public class Solution {
    public int[] singleNumber(int[] nums) {
        int[] res = new int[2];
        if(nums == null || nums.length < 2) return res;
        int val = 0;
        for(int num : nums){
            val ^= num;
        }
        int index = 0;
        while(val != 0){
            if((val & 1) == 1) break;
            else{
                val = val >>> 1;
                index++;
            }
        }
        int num1 = 0;
        int num2 = 0;
        for(int num : nums){
            if(((num >>> index) & 1) == 1){
                num1 ^= num;
            }
            else{
                num2 ^= num;
            }
        }
        res[0] = num1;
        res[1] = num2;
        return res;
    }
}