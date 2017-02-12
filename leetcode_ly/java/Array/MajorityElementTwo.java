public class Solution {
    public List<Integer> majorityElement(int[] nums) {
        List<Integer> res = new ArrayList<Integer>();
        if(nums.length == 0) return res;
        int num1 = nums[0];
        int num2 = nums[0];
        int cnt1 = 0;
        int cnt2 = 0;
        
        for(int i = 0; i < nums.length; i++){
            if(num1 == nums[i]) cnt1++;
            else if(num2 == nums[i]) cnt2++;
            else if(cnt1 == 0){
                num1 = nums[i];
                cnt1 = 1;
            }
            else if(cnt2 == 0){
                num2 = nums[i];
                cnt2 = 1;
            }
            else{
                cnt1--;
                cnt2--;
            }
        }
        
        cnt1 = 0;
        cnt2 = 0;
        for(int i = 0; i < nums.length; i++){
            if(nums[i] == num1) cnt1++;
            //if(nums[i] == num2 && num1 != num2) cnt2++;
            else if(nums[i] == num2) cnt2++;
        }
        if(cnt1 > nums.length / 3) res.add(num1);
        if(cnt2 > nums.length / 3) res.add(num2);
        return res;
    }
}

map