public class Solution {
    public String longestCommonPrefix(String[] strs) {
        if(strs.length == 0) return "";
        if(strs.length == 1) return strs[0];
        int len = strs[0].length();
        for(int i = 1; i < strs.length; i++){
            if(strs[i].length() < len) len = strs[i].length();
        }
        
        for(int i = len; i > 0; i--){
            int j = 1;
            for(; j < strs.length; j++){
                if(!strs[j].substring(0,i).equals(strs[j - 1].substring(0,i))) break;
                else continue;
            }
            if(j == strs.length) return strs[j - 1].substring(0,i);
        }
        return "";
    }
}