public class Solution {
    public List<String> generateParenthesis(int n) {
        List<String> res = new ArrayList<String>();
        if(n == 0) return res;
        generate(res,"",n,n);
        return res;
    }
    
    public void generate(List<String> res,String ans,int left,int right){
        if(left > right) return;
        if(left == 0 && right == 0){
            res.add(ans);
            return;
        }
        
        if(left > 0){
            ans = ans + '(';
            generate(res,ans, left - 1,right);
            ans = ans.substring(0,ans.length() - 1);
        }
        if(right > 0){
            ans = ans + ')';
            generate(res,ans, left,right - 1);
            ans = ans.substring(0,ans.length() - 1);
        }
    }
}