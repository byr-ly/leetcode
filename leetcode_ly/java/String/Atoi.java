public class Solution {
    public int myAtoi(String str) {
        int sign = 1;
        int sum = 0;
        int index = 0;
        //1.ÅÐ¿Õ
        if(str.isEmpty()) return 0;
        
        //2.³ý¿Õ¸ñ
        str = str.trim();
        
        //3.¶¨·ûºÅ
        if(index < str.length() && str.charAt(index) == '+') index++;
        else if(index < str.length() && str.charAt(index) == '-'){
            sign = -1;
            index++;
        }
        while(index < str.length()){
            int digit = str.charAt(index) - '0';
            if(digit > 9 || digit < 0) break;
            
            if(sum > Integer.MAX_VALUE / 10 || (sum == Integer.MAX_VALUE / 10 && digit > 7)){
                return (sign == 1) ? Integer.MAX_VALUE : Integer.MIN_VALUE;
            }
            sum = sum * 10 + digit;
            index++;
        }
        return sign * sum;
    }
}