/**
 * // This is the interface that allows for creating nested lists.
 * // You should not implement it, or speculate about its implementation
 * public interface NestedInteger {
 *
 *     // @return true if this NestedInteger holds a single integer, rather than a nested list.
 *     public boolean isInteger();
 *
 *     // @return the single integer that this NestedInteger holds, if it holds a single integer
 *     // Return null if this NestedInteger holds a nested list
 *     public Integer getInteger();
 *
 *     // @return the nested list that this NestedInteger holds, if it holds a nested list
 *     // Return null if this NestedInteger holds a single integer
 *     public List<NestedInteger> getList();
 * }
 */
public class NestedIterator implements Iterator<Integer> {

    public NestedIterator(List<NestedInteger> nestedList) {
        for(int i = nestedList.size() - 1; i >= 0; i--){
            s.push(nestedList.get(i));
        }
    }

    @Override
    public Integer next() {
        return s.pop().getInteger();
    }

    @Override
    public boolean hasNext() {
        while(!s.isEmpty()){
            NestedInteger n = s.peek();
            if(n.isInteger()) return true;
            else{
                List<NestedInteger> list = s.pop().getList();
                for(int j = list.size() - 1; j >= 0; j--){
                    s.push(list.get(j));
                }
            }
        }
        return false;
    }
    
    private Stack<NestedInteger> s = new Stack<NestedInteger>();
}

/**
 * Your NestedIterator object will be instantiated and called as such:
 * NestedIterator i = new NestedIterator(nestedList);
 * while (i.hasNext()) v[f()] = i.next();
 */