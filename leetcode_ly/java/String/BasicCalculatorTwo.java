public class Solution {
    public int calculate(String s) {
        if(s == null || s.isEmpty()) return 0;
        Stack<Integer> stack = new Stack<Integer>();
        int num = 0;
        //�жϵ�һ�������������Ǹ���
        char sign = (s.charAt(0) == '-') ? '-' : '+';
        int i = (sign == '-') ? 1 : 0;
        while(i < s.length()){
            if(Character.isDigit(s.charAt(i))){
                num = num * 10 + (s.charAt(i) - '0');
            }
            //ֻ��һ���������
            if((!Character.isDigit(s.charAt(i)) && s.charAt(i) != ' ') || i == s.length() - 1){
                if(sign == '+'){
                    stack.push(num);
                }
                else if(sign == '-'){
                    stack.push(-num);
                }
                else if(sign == '*'){
                    stack.push(stack.pop() * num);
                }
                else if(sign == '/'){
                    stack.push(stack.pop() / num);
                }
                sign = s.charAt(i);
                num = 0;
            }
            i++;
        }
        int res = 0;
        for(int j : stack){
            res += j;
        }
        return res;
    }
}