public class Solution {
    public int longestConsecutive(int[] nums) {
        HashSet<Integer> set = new HashSet<Integer>();
        for(int i = 0; i < nums.length; i++){
            set.add(nums[i]);
        }
        int sum = Integer.MIN_VALUE;
        
        for(int i = 0; i < nums.length; i++){
            //升序个数
            int upCnt = findNum(set,nums[i],true);
            //降序个数
            int downCnt = findNum(set,nums[i] - 1,false);
            sum = Math.max(sum,upCnt + downCnt);
        }
        return sum;
    }
    
    public int findNum(HashSet<Integer> set,int num,boolean flag){
        int cnt = 0;
        while(set.contains(num)){
            cnt++;
            //同一序列中的数得到的结果是一样的  这样可以减少时间复杂度
            set.remove(num);
            if(flag) num++;
            else num--;
        }
        return cnt;
    }
}