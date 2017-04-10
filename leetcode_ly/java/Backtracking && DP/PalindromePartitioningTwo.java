public class Solution {
    public int minCut(String s) {
        if(s == null || s.length() == 0) return 0;
        //cut数组记录在每个索引最少需要切分的次数；pal数组记录j-i是否为palindrome
        int[] cut = new int[s.length()];
        boolean[][] pal = new boolean[s.length()][s.length()];
        for(int i = 0; i < s.length(); i++){
            int min = i;
            for(int j = 0; j <= i; j++){
                if(s.charAt(i) == s.charAt(j) && (j + 1 > i - 1 || pal[j + 1][i - 1])){
                    pal[j][i] = true;
                    min = j == 0 ? 0 : Math.min(min,cut[j - 1] + 1);
                }
            }
            cut[i] = min;
        }
        return cut[s.length() - 1];
    }
}