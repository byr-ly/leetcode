class MyQueue {
    // Push element x to the back of queue.
    public void push(int x) {
        s1.push(x);
    }

    // Removes the element from in front of queue.
    public void pop() {
        while(!s1.isEmpty()){
            int val = s1.pop();
            s2.push(val);
        }
        s2.pop();
        while(!s2.isEmpty()){
            int val = s2.pop();
            s1.push(val);
        }
    }

    // Get the front element.
    public int peek() {
        while(!s1.isEmpty()){
            int val = s1.pop();
            s2.push(val);
        }
        int value = s2.peek();
        while(!s2.isEmpty()){
            int val = s2.pop();
            s1.push(val);
        }
        return value;
    }

    // Return whether the queue is empty.
    public boolean empty() {
        return s1.isEmpty();
    }
    
    private Stack<Integer> s1 = new Stack<Integer>();
    private Stack<Integer> s2 = new Stack<Integer>();
}