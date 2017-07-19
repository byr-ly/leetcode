public class Solution {
    public List<String> readBinaryWatch(int num) {
        List<String> res = new ArrayList<>();
        int[] nums1 = {1,2,4,8};
        int[] nums2 = {1,2,4,8,16,32};
        for(int i = 0; i <= num; i++){
            List<Integer> hour = new ArrayList<>();
            List<Integer> minute = new ArrayList<>();
            generate(hour,nums1,i,0,0,0);
            generate(minute,nums2,num - i,0,0,0);
            for(int h : hour){
                for(int m : minute){
                    if(h >= 12 || m >= 60) continue;
                    res.add(h + ":" + ((m < 10) ? "0" + m : m));
                }
            }
        }
        return res;
    }
    
    public void generate(List<Integer> ans,int[] nums,int k,int count,int sum,int start){
        if(count == k){
            ans.add(sum);
            return;
        }
        
        for(int i = start; i < nums.length; i++){
            sum += nums[i];
            count++;
            generate(ans,nums,k,count,sum,i + 1);
            sum -= nums[i];
            count--;
        }
    }
}