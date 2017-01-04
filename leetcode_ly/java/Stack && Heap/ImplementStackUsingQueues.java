class MyStack {
    // Push element x onto stack.
    public void push(int x) {
        q1.add(x);
    }

    // Removes the element on top of the stack.
    public void pop() {
        while(q1.size() != 1){
            int val = q1.poll();
            q2.add(val);
        }
        q1.poll();
        while(!q2.isEmpty()){
            int val = q2.poll();
            q1.add(val);
        }
    }

    // Get the top element.
    public int top() {
        while(q1.size() != 1){
            int val = q1.poll();
            q2.add(val);
        }
        int value = q1.poll();
        while(!q2.isEmpty()){
            int val = q2.poll();
            q1.add(val);
        }
        q1.add(value);
        return value;
    }

    // Return whether the stack is empty.
    public boolean empty() {
        return q1.isEmpty();
    }
    
    private Queue<Integer> q1 = new LinkedList<Integer>();
    private Queue<Integer> q2 = new LinkedList<Integer>();
}