public class Solution {
    public int candy(int[] ratings) {
        if(ratings.length == 0) return 0;
        int[] count = new int[ratings.length];
        //给每个孩子一个糖
        Arrays.fill(count,1);
        //只要保证和相邻的比较，因此两趟比较即可完成
        for(int i = 1; i < ratings.length; i++){
            if(ratings[i] > ratings[i - 1]) count[i] = count[i - 1] + 1;
        }
        
        for(int i = count.length - 2; i >= 0; i--){
            if(ratings[i] > ratings[i + 1]) count[i] = Math.max(count[i],count[i + 1] + 1);
        }
        int sum = 0;
        for(int i : count){
            sum += i;
        }
        return sum;
    }
}