public class Solution {
    public int minPatches(int[] nums, int n) {
        //sum标记可达区间的右区间，开区间
        long sum = 1;//为了防止最后越界将sun设置为long
        int i = 0;
        int count = 0;
        while(sum <= n){
            if(i < nums.length && sum >= nums[i]){
                sum += nums[i];
                i++;
            }
            else{
                //表示1---sum - 1都可达,所以要补个sum
                sum += sum;
                count++;
            }
        }
        return count;
    }
}