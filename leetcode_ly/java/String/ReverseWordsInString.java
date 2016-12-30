public class Solution {
    public String reverseWords(String s) {
        s = s.trim();
        if(s.equals("")) return "";
        String[] list = s.split("\\s+");
        int i = 0;
        int j = list.length - 1;
        while(i < j){
            String ss = list[i];
            list[i] = list[j];
            list[j] = ss;
            i++;
            j--;
        }
        StringBuffer sb = new StringBuffer();
        for(int k = 0; k < list.length; k++){
            sb.append(list[k]).append(" ");
        }
        return sb.substring(0,sb.length() - 1).toString();
    }
}