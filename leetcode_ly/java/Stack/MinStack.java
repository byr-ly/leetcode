public class MinStack {

    /** initialize your data structure here. */
    public MinStack() {
        s = new Stack<Integer>();
        min = Integer.MAX_VALUE;
    }
    
    public void push(int x) {
        if(x <= min){
            //先将最小值push进来以保存上一个最小值
            s.push(min);
            min = x;
        }
        s.push(x);
    }
    
    public void pop() {
        if(min == s.pop()) min = s.pop();
    }
    
    public int top() {
        return s.peek();
    }
    
    public int getMin() {
        return min;
    }
    
    private Stack<Integer> s;
    private int min;
}

/**
 * Your MinStack object will be instantiated and called as such:
 * MinStack obj = new MinStack();
 * obj.push(x);
 * obj.pop();
 * int param_3 = obj.top();
 * int param_4 = obj.getMin();
 */