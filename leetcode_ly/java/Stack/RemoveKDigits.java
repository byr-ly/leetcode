public class Solution {
    public String removeKdigits(String num, int k) {
        if(k == num.length()) return "0";
        Stack<Character> s = new Stack<Character>();
        int i = 0;
        while(i < num.length()){
            while(!s.isEmpty() && k > 0 && num.charAt(i) < s.peek()){
                s.pop();
                k--;
            }
            s.push(num.charAt(i));
            i++;
        }
        
        StringBuffer str = new StringBuffer();
        while(!s.isEmpty()){
            str = str.append(s.pop());
        }
        str = str.reverse();
        int j = 0;
        while(j < str.length() && str.charAt(j) == '0'){
            j++;
        }
        //有可能因为后面有递增序列k没有减为0
        if(k > 0) return str.substring(0,str.length() - k);
        //将前面的0去掉后有可能为""
        return str.substring(j).equals("") ? "0" : str.substring(j);
    }
}