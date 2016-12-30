public class Solution {
    public int minDistance(String word1, String word2) {
        int m = word1.length();
        int n = word2.length();
        int[][] cost = new int[m + 1][n + 1];//word1��ǰi���ַ�ת���word2��ǰj���ַ�����Ҫ�Ĳ���
        
        for(int i = 0; i <= m; i++){
            cost[i][0] = i;
        }
        for(int j = 0; j <= n; j++){
            cost[0][j] = j;
        }
        
        for(int i = 0; i < m; i++){
            for(int j = 0; j < n; j++){
                if(word1.charAt(i) == word2.charAt(j)){
                    cost[i + 1][j + 1] = cost[i][j];//��ͬ��f(i,j) = f(i - 1,j - 1)
                }
                else{
                    int a = cost[i][j + 1];//ɾ��
                    int b = cost[i + 1][j];//����
                    int c = cost[i][j];//�滻
                    cost[i + 1][j + 1] = a < b ? (a < c ? a : c) : (b < c ? b : c);
                    cost[i + 1][j + 1]++;
                }
            }
        }
        return cost[m][n];
    }
}