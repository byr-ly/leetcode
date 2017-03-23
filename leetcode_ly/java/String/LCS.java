/**
 * LCS
 */
public class App {
    public static void main(String[] args) {
        String str = "absjhfis";
        String ptr = "bjkas";
        String res = LCS(str,ptr);
        System.out.println(res);
    }

    public static String LCS(String str,String ptr){
        StringBuffer s = new StringBuffer();
        int[][] dp = new int[str.length() + 1][ptr.length() + 1];
        for(int i = 1; i < dp.length; i++){
            for(int j = 1; j < dp[0].length; j++){
                if(str.charAt(i - 1) == ptr.charAt(j - 1)){
                    dp[i][j] = dp[i - 1][j - 1] + 1;
                }
                else{
                    dp[i][j] = Math.max(dp[i - 1][j],dp[i][ j - 1]);
                }
            }
        }

        int i = str.length();
        int j = ptr.length();
        while(i > 0 && j > 0){
            if(str.charAt(i - 1) == ptr.charAt(j - 1)){
                s = s.append(str.charAt(i - 1));
                i--;
                j--;
            }
            else{
                if(dp[i][j - 1] > dp[i - 1][j]) j--;
                else i--;
            }
        }
        return s.reverse().toString();
    }
}
