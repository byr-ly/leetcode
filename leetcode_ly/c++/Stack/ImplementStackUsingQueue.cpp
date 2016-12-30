class Stack {
public:
    // Push element x onto stack.
    void push(int x) {
        q.push(x);
    }

    // Removes the element on top of the stack.
    void pop() {
        if(!q.empty()){
            while(q.size() != 1){
                p.push(q.front());
                q.pop();
            }
        }
        q.pop();
        while(!p.empty()){
            q.push(p.front());
            p.pop();
        }
    }

    // Get the top element.
    int top() {
        if(!q.empty()){
            while(q.size() != 1){
                p.push(q.front());
                q.pop();
            }
        }
        int x = q.front();
        p.push(x);
        q.pop();
        while(!p.empty()){
            q.push(p.front());
            p.pop();
        }
        return x;
    }

    // Return whether the stack is empty.
    bool empty() {
        return q.empty();
    }
    
private:
    queue<int> q;
    queue<int> p;
};