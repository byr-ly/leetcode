public class Solution {
    public String decodeString(String s) {
        StringBuffer res = new StringBuffer();
        Stack<Character> stack = new Stack<Character>();
        int i = 0;
        while(i < s.length()){
            if(s.charAt(i) != ']') stack.push(s.charAt(i));
            else{
                //获取被重复字符串
                StringBuffer cur = new StringBuffer();
                while(stack.peek() != '['){
                    cur = cur.append(stack.pop());
                }
                cur = cur.reverse();
                stack.pop();
                //获取重复的次数
                StringBuffer digit = new StringBuffer();
                while(!stack.isEmpty() && Character.isDigit(stack.peek())){
                    digit = digit.append(stack.pop());
                }
                digit = digit.reverse();
                int cnt = Integer.parseInt(digit.toString());
                //连接然后入栈
                StringBuffer str = new StringBuffer();
                while(cnt != 0){
                    str = str.append(cur);
                    cnt--;
                }
                for(int j = 0; j < str.length(); j++){
                    stack.push(str.charAt(j));
                }
            }
            i++;
        }
        while(!stack.isEmpty()){
            res = res.append(stack.pop());
        }
        return res.reverse().toString();
    }
}