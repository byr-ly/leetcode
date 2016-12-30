/**
 * // This is the interface that allows for creating nested lists.
 * // You should not implement it, or speculate about its implementation
 * public interface NestedInteger {
 *     // Constructor initializes an empty nested list.
 *     public NestedInteger();
 *
 *     // Constructor initializes a single integer.
 *     public NestedInteger(int value);
 *
 *     // @return true if this NestedInteger holds a single integer, rather than a nested list.
 *     public boolean isInteger();
 *
 *     // @return the single integer that this NestedInteger holds, if it holds a single integer
 *     // Return null if this NestedInteger holds a nested list
 *     public Integer getInteger();
 *
 *     // Set this NestedInteger to hold a single integer.
 *     public void setInteger(int value);
 *
 *     // Set this NestedInteger to hold a nested list and adds a nested integer to it.
 *     public void add(NestedInteger ni);
 *
 *     // @return the nested list that this NestedInteger holds, if it holds a nested list
 *     // Return null if this NestedInteger holds a single integer
 *     public List<NestedInteger> getList();
 * }
 */
 
 /**
  * If encounters '[', push current NestedInteger to stack and start a new one.
    If encounters ']', end current NestedInteger and pop a NestedInteger from stack to continue.
    If encounters ',', append a new number to curr NestedInteger, if this comma is not right after a brackets.
    Update index l and r, where l shall point to the start of a integer substring, while r shall points to the end+1 of substring.
*/
public class Solution {
    public NestedInteger deserialize(String s) {
        if(s == null || s.isEmpty()) return null;
        if(s.charAt(0) != '[') return new NestedInteger(Integer.parseInt(s));
        
        int l = 0;
        NestedInteger cur = null;
        Stack<NestedInteger> stack = new Stack<NestedInteger>();
        for(int r = 0; r < s.length(); r++){
            if(s.charAt(r) == '['){
                if(cur != null) stack.push(cur);
                cur = new NestedInteger();
                l = r + 1;
            }
            else if(s.charAt(r) == ','){
                if(s.charAt(r - 1) != ']'){
                    String num = s.substring(l,r);
                    cur.add(new NestedInteger(Integer.parseInt(num)));   
                }
                l = r + 1;
            }
            else if(s.charAt(r) == ']'){
                String num = s.substring(l,r);
                if(!num.isEmpty()){
                    cur.add(new NestedInteger(Integer.parseInt(num)));
                }
                if(!stack.isEmpty()){
                    NestedInteger p = stack.pop();
                    p.add(cur);
                    cur = p;
                }
                l = r + 1;
            }
        }
        return cur;
    }
}