public class Solution {
    public int minPatches(int[] nums, int n) {
        //sum��ǿɴ�����������䣬������
        long sum = 1;//Ϊ�˷�ֹ���Խ�罫sun����Ϊlong
        int i = 0;
        int count = 0;
        while(sum <= n){
            if(i < nums.length && sum >= nums[i]){
                sum += nums[i];
                i++;
            }
            else{
                //��ʾ1---sum - 1���ɴ�,����Ҫ����sum
                sum += sum;
                count++;
            }
        }
        return count;
    }
}