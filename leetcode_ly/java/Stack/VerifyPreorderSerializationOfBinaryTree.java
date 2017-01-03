public class Solution {
    public boolean isValidSerialization(String preorder) {
        String[] fields = preorder.split(",");
        Stack<String> s = new Stack<String>();
        for(int i = 0; i < fields.length; i++){
            if(!fields[i].equals("#")) s.push(fields[i]);
            else{
                while(!s.isEmpty() && s.peek().equals("#")){
                    s.pop();
                    if(s.isEmpty()) return false;
                    s.pop();
                }
                s.push(fields[i]);
            }
        }
        if(s.size() == 1 && s.pop().equals("#")) return true;
        else return false;
    }
}