class Queue {
public:
    // Push element x to the back of queue.
    void push(int x) {
        in.push(x);
    }

    // Removes the element from in front of queue.
    void pop(void) {
        if(!in.empty()){
            while(!in.empty()){
                out.push(in.top());
                in.pop();
            }
            out.pop();
        }
        while(!out.empty()){
            in.push(out.top());
            out.pop();
        }
    }

    // Get the front element.
    int peek(void) {
        if(!in.empty()){
            while(!in.empty()){
                out.push(in.top());
                in.pop();
            }
        }
        int x = out.top();
        while(!out.empty()){
            in.push(out.top());
            out.pop();
        }
        return x;
    }

    // Return whether the queue is empty.
    bool empty(void) {
        return in.empty() && out.empty();
    }
    
private:
    stack<int> in;
    stack<int> out;
};