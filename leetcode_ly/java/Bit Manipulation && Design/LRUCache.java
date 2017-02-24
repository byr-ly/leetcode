class Node{
    int key;
    int value;
    Node pre;
    Node next;
    
    public Node(int key,int value){
        this.key = key;
        this.value = value;
    }
}

public class LRUCache {
    HashMap<Integer,Node> map;
    Node head;
    Node tail;
    int capacity;
    int count;

    public LRUCache(int capacity) {
        map = new HashMap<Integer,Node>();
        head = new Node(0,0);
        tail = new Node(0,0);
        this.capacity = capacity;
        count = 0;
        head.pre = null;
        head.next = tail;
        tail.pre = head;
        tail.next = null;
    }
    
    public void addToHead(Node node){
        node.next = head.next;
        node.next.pre = node;
        node.pre = head;
        head.next = node;
    }
    
    public void remove(Node node){
        node.next.pre = node.pre;
        node.pre.next = node.next;
    }
    
    public int get(int key) {
        if(map.containsKey(key)){
            Node node = map.get(key);
            int result = node.value;
            remove(node);
            addToHead(node);
            return result;
        }
        return -1;
    }
    
    public void put(int key, int value) {
        if(!map.containsKey(key)){
            Node node = new Node(key,value);
            if(count < capacity){
                map.put(key,node);
                count++;
                addToHead(node);
            }
            else{
                map.remove(tail.pre.key);
                remove(tail.pre);
                map.put(key,node);
                addToHead(node);
            }
        }
        else{
            Node node = map.get(key);
            node.value = value;
            remove(node);
            addToHead(node);
        }
    }
}

/**
 * Your LRUCache object will be instantiated and called as such:
 * LRUCache obj = new LRUCache(capacity);
 * int param_1 = obj.get(key);
 * obj.put(key,value);
 */