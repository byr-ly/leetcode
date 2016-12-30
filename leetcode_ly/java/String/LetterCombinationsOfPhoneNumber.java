public class Solution {
    public List<String> letterCombinations(String digits) {
        HashMap<Integer,String> map = new HashMap<Integer,String>();
        map.put(2,"abc");
        map.put(3,"def");
        map.put(4,"ghi");
        map.put(5,"jkl");
        map.put(6,"mno");
        map.put(7,"pqrs");
        map.put(8,"tuv");
        map.put(9,"wxyz");
        
        ArrayList<String> list = new ArrayList<String>();
        for(int i = 0; i < digits.length(); i++){
            list.add(map.get(digits.charAt(i) - '0'));
        }
        
        List<String> res = new ArrayList<String>();
        if(digits.equals("")) return res;
        dfs(res,"",list,0);
        return res;
    }
    
    public void dfs(List<String> res,String ans,ArrayList<String> list,int start){
        if(start == list.size()){
            res.add(ans);
            return;
        }
        
        for(int i = start; i < list.size(); i++){
            String s = list.get(i);
            for(int j = 0; j < s.length(); j++){
                ans += s.charAt(j);
                dfs(res,ans,list,i + 1);
                ans = ans.substring(0,ans.length() - 1);
            }
            //强制返回第一层，否则会继续循环i++
            return;
        }
    }
}