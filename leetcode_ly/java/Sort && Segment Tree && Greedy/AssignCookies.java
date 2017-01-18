public class Solution {
    public int findContentChildren(int[] g, int[] s) {
        if(g.length == 0 || s.length == 0) return 0;
        Arrays.sort(g);
        Arrays.sort(s);
        
        int res = 0;
        for(int i = 0; i < g.length; i++){
            for(int j = 0; j < s.length; j++){
                if(s[j] >= g[i]){
                    res++;
                    s[j] = 0;
                    break;
                }
            }
        }
        return res;
    }
}