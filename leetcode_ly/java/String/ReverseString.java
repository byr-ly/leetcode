public class Solution {
    public String reverseString(String s) {
        if(s.isEmpty()) return s;
        StringBuffer ss = new StringBuffer(s);
        int i = 0;
        int j = s.length() - 1;
        while(i < j){
            char temp = ss.charAt(i);
            ss.setCharAt(i,ss.charAt(j));
            ss.setCharAt(j,temp);
            i++;
            j--;
        }
        return ss.toString();
    }
}