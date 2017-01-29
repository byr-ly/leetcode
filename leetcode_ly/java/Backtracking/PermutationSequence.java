//这种方法会超时，可以利用数学手段根据所要求的k判断具体的字符顺序
public class Solution {
    public String getPermutation(int n, int k) {
        List<String> res = new ArrayList<String>();
        getResult(res,n,"");
        System.out.println(res.get(k - 1));
        return res.get(k - 1);
    }
    
    public void getResult(List<String> res,int n,String ans){
        if(ans.length() == n){
            res.add(ans);
            return;
        }
        
        for(int i = 1; i <= n; i++){
            if(ans.indexOf((char)('0' + i)) != -1) continue;
            ans += i;
            getResult(res,n,ans);
            ans = ans.substring(0,ans.length() - 1);
        }
    }
}