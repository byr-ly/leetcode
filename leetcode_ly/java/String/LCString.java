/**
 * LCString
 */
public class App {
    public static void main(String[] args) {
        String str = "adbccadebbca";
        String ptr = "edabccadece";
        String res = LCS(str,ptr);
        System.out.println(res);
    }

    public static String LCS(String str,String ptr){
        StringBuffer s = new StringBuffer();
        int maxLen = Integer.MIN_VALUE;
        int row = 0;
        int col = 0;
        int[][] dp = new int[str.length()][ptr.length()];
        for(int i = 0; i < dp.length; i++){
            for(int j = 0; j < dp[0].length; j++){
                if(str.charAt(i) == ptr.charAt(j)){
                    if(i == 0 || j == 0) dp[i][j] = 1;
                    else dp[i][j] = dp[i - 1][j - 1] + 1;
                    if(dp[i][j] > maxLen){
                        maxLen = dp[i][j];
                        row = i;
                        col = j;
                    }
                }
            }
        }

        while(row >= 0 && col >= 0 && maxLen > 0){
            s = s.append(str.charAt(row));
            row--;
            col--;
            maxLen--;
        }
        return s.reverse().toString();
    }
}