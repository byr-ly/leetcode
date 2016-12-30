public class Solution {
    public int numDistinct(String s, String t) {
        int[][] m = new int[t.length() + 1][s.length() + 1];
        for(int i = 0; i <= t.length(); i++){
            m[i][0] = 0;
        }
        for(int j = 0; j <= s.length(); j++){
            m[0][j] = 1;
        }
        
        for(int i = 0; i < t.length(); i++){
            for(int j = 0; j < s.length(); j++){
                if(t.charAt(i) != s.charAt(j)) m[i + 1][j + 1] = m[i + 1][j];
                else m[i + 1][j + 1] = m[i][j] + m[i + 1][j];
            }
        }
        return m[t.length()][s.length()];
    }
}