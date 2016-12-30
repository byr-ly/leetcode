class MinStack {
public:
    /** initialize your data structure here. */
    MinStack() {
        
    }
    
    void push(int x) {
       in.push(x);
       if(min.empty() || x <= min.top()) min.push(x);
    }
    
    void pop() {
        if(in.top() == min.top()){
            min.pop();
        }
        in.pop();
    }
    
    int top() {
        return in.top();
    }
    
    int getMin() {
        return min.top();
    }
    
private:
    stack<int> in;
    stack<int> min;
};

/**
 * Your MinStack object will be instantiated and called as such:
 * MinStack obj = new MinStack();
 * obj.push(x);
 * obj.pop();
 * int param_3 = obj.top();
 * int param_4 = obj.getMin();
 */