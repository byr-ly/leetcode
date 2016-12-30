public class Solution {
    public List<String> restoreIpAddresses(String s) {
        List<String> res = new ArrayList<String>();
        if(s == null || s.isEmpty()) return res;
        dfs(res,s,"",0,0);
        return res;
    }
    
    //ans表示每个符合要求的solution，idx用来截取每段地址，i表示每段地址的长度，count表示第几段
    public void dfs(List<String> res,String s,String ans,int idx,int count){
        if(count > 4) return;
        if(count == 4 && idx == s.length()){
            res.add(ans);
            return;
        }
        
        for(int i = 1; i < 4; i++){
            if(idx + i > s.length()) break;
            String str = s.substring(idx,idx + i);
            if((str.charAt(0) == '0' && str.length() != 1) || (str.length() == 3 && Integer.parseInt(str) > 255)) continue;
            dfs(res,s,ans + str + ((count == 3) ? "" : "."),idx + i,count + 1);
        }
    }
}