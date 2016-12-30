public class Solution {
    public String simplifyPath(String path) {
        String[] str = path.split("/");
        Stack<String> stack = new Stack<String>();
        for(int i = 0; i < str.length; i++){
            if(!stack.isEmpty() && str[i].equals("..")) stack.pop();
            else if(!str[i].equals(".") && !str[i].equals("") && !str[i].equals("..")) stack.push(str[i]);
        }
        ArrayList<String> list = new ArrayList<String>(stack);
        return "/" + String.join("/",list);
    }
}