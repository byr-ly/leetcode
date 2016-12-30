public class Solution {
    public boolean repeatedSubstringPattern(String str) {
        int len = str.length();
        for(int i = len / 2; i >= 1; i--){
            if(len % i == 0){
                int m = len / i;
                StringBuffer s = new StringBuffer();
                String base = str.substring(0,i);
                for(int j = 0; j < m; j++){
                    s.append(base);
                }
                if(s.toString().equals(str)) return true;
            }
        }
        return false;
    }
}