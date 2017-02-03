public class Solution {
    public int findSubstringInWraproundString(String p) {
        if(p.length() == 0) return 0;
        int[] dp = new int[26];//标记以每个字符结尾的字符串有多少个
        int maxLength = 0;
        //如果是连续的，就直接加1，否则重置
        for(int i = 0; i < p.length(); i++){
            if(i > 0 && ((p.charAt(i) - p.charAt(i - 1) == 1) || (p.charAt(i - 1) - p.charAt(i) == 25))){
                maxLength++;
            }
            else{
                maxLength = 1;
            }
            int index = p.charAt(i) - 'a';
            //防止后面相同的字符覆盖之前的结果
            dp[index] = Math.max(dp[index],maxLength);
        }
        
        int sum = 0;
        for(int i : dp){
            sum += i;
        }
        return sum;
    }
}