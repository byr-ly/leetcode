public class Solution {
    public String reverseVowels(String s) {
        if(s.length() == 0) return s;
        StringBuffer ss = new StringBuffer(s);
        int low = 0;
        int high = s.length() - 1;
        
        while(low < high){
            while(low < high && ss.charAt(low) != 'a' && ss.charAt(low) != 'e' && ss.charAt(low) != 'i' && ss.charAt(low) != 'o' && ss.charAt(low) != 'u'&& ss.charAt(low) != 'A' && ss.charAt(low) != 'E' && ss.charAt(low) != 'I' && ss.charAt(low) != 'O' && ss.charAt(low) != 'U'){
                low++;
            }
            while(low < high && ss.charAt(high) != 'a' && ss.charAt(high) != 'e' && ss.charAt(high) != 'i' && ss.charAt(high) != 'o' && ss.charAt(high) != 'u' && ss.charAt(high) != 'A' && ss.charAt(high) != 'E' && ss.charAt(high) != 'I' && ss.charAt(high) != 'O' && ss.charAt(high) != 'U'){
                high--;
            }
            
            char temp = ss.charAt(low);
            ss.setCharAt(low,s.charAt(high));
            ss.setCharAt(high,temp);
            
            low++;
            high--;
        }
        return ss.toString();
    }
}