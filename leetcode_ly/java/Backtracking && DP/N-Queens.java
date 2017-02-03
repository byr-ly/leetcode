public class Solution {
    public List<List<String>> solveNQueens(int n) {
        List<List<String>> res = new ArrayList<List<String>>();
        char[][] ans = new char[n][n];
        for(int i = 0; i < n; i++){
            for(int j = 0; j < n; j++){
                ans[i][j] = '.';
            }
        }
        getResult(res,ans,0);
        return res;
    }
    
    public void getResult(List<List<String>> res,char[][] ans,int k){
        int n = ans.length;
        if(k == n){
            res.add(construct(ans));
            return;
        }
        
        //k表示列，根据每一列进行搜索，因此列肯定不相同，因此下面不用判断列
        for(int i = 0; i < n; i++){
            if(isValid(i,k,ans)){
                ans[i][k] = 'Q';
                getResult(res,ans,k + 1);
                ans[i][k] = '.';
            }
        }
    }
    
    public boolean isValid(int x,int y,char[][] ans){
        for(int i = 0; i < ans.length; i++){
            for(int j = 0; j < ans[0].length; j++){
                if(ans[i][j] == 'Q' && (x - i == y - j || x - i == j - y || x == i)){
                    return false;
                }
            }
        }
        return true;
    }
    
    public List<String> construct(char[][] ans){
        List<String> res = new ArrayList<String>();
        for(int i = 0; i < ans.length; i++){
            String s = new String(ans[i]);
            res.add(s);
        }
        return res;
    }
}