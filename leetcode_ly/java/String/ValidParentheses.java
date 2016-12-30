public class Solution {
    public boolean isValid(String s) {
        if(s.equals("")) return true;
        Stack<Character> stack = new Stack<Character>();
        stack.push(s.charAt(0));
        int i = 1;
        while(i < s.length()){
            if(stack.isEmpty()){
                stack.push(s.charAt(i));
                i++;
            }
            else{
                switch(s.charAt(i)){
                    case ')':
                        if(stack.peek() == '(') stack.pop();
                        else stack.push(s.charAt(i));
                        break;
                    case ']':
                        if(stack.peek() == '[') stack.pop();
                        else stack.push(s.charAt(i));
                        break;
                    case '}':
                        if(stack.peek() == '{') stack.pop();
                        else stack.push(s.charAt(i));
                        break;
                    default:
                        stack.push(s.charAt(i));
                        break;
                }
                i++;
            }
        }
        return stack.isEmpty();
    }
}