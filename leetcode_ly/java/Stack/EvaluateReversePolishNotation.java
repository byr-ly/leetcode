public class Solution {
    public int evalRPN(String[] tokens) {
        Stack<Integer> s = new Stack<Integer>();
        for(int i = 0; i < tokens.length; i++){
            if(!tokens[i].equals("+") && !tokens[i].equals("-") && !tokens[i].equals("*") && !tokens[i].equals("/")){
                s.push(Integer.parseInt(tokens[i]));
            }
            else{
                int b = s.pop();
                int a = s.pop();
                if(tokens[i].equals("+")) s.push(a + b);
                else if(tokens[i].equals("-")) s.push(a - b);
                else if(tokens[i].equals("*")) s.push(a * b);
                else s.push(a / b);
            }
        }
        return s.pop();
    }
}