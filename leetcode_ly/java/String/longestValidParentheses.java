public class Solution {
    public int longestValidParentheses(String s) {
        if(s == null || s.isEmpty()) return 0;
        Stack<Integer> stack = new Stack<Integer>();
        int i = 0;
        while(i < s.length()){
            if(s.charAt(i) == '(') stack.push(i);
            else{
                if(!stack.isEmpty()){
                    if(s.charAt(stack.peek()) == '(') stack.pop();
                    else stack.push(i);
                }
                else stack.push(i);
            }
            i++;
        }
        if(stack.isEmpty()) return s.length();
        
        int longest = 0;
        int b = s.length();
        while(!stack.isEmpty()){
            int a = stack.pop();
            longest = Math.max(longest,b - a - 1);
            b = a;
        }
        longest = Math.max(longest,b);
        return longest;
    }
}