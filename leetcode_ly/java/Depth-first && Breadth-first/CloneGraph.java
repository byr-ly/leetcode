/**
 * Definition for undirected graph.
 * class UndirectedGraphNode {
 *     int label;
 *     List<UndirectedGraphNode> neighbors;
 *     UndirectedGraphNode(int x) { label = x; neighbors = new ArrayList<UndirectedGraphNode>(); }
 * };
 */
public class Solution {
    HashMap<Integer,UndirectedGraphNode> map = new HashMap<Integer,UndirectedGraphNode>();
    public UndirectedGraphNode cloneGraph(UndirectedGraphNode node) {
        return clone(node);
    }
    
    public UndirectedGraphNode clone(UndirectedGraphNode node){
        if(node == null) return null;
        if(map.containsKey(node.label)) return map.get(node.label);
        UndirectedGraphNode head = new UndirectedGraphNode(node.label);
        map.put(head.label,head);
        for(int i = 0; i < node.neighbors.size(); i++){
            head.neighbors.add(clone(node.neighbors.get(i)));
        }
        return head;
    }
}

//这样不分为两个函数也可以，但是效率明显比上面低
/**
 * Definition for undirected graph.
 * class UndirectedGraphNode {
 *     int label;
 *     List<UndirectedGraphNode> neighbors;
 *     UndirectedGraphNode(int x) { label = x; neighbors = new ArrayList<UndirectedGraphNode>(); }
 * };
 */
public class Solution {
    HashMap<Integer,UndirectedGraphNode> map = new HashMap<Integer,UndirectedGraphNode>();
    public UndirectedGraphNode cloneGraph(UndirectedGraphNode node) {
        if(node == null) return null;
        if(map.containsKey(node.label)) return map.get(node.label);
        UndirectedGraphNode head = new UndirectedGraphNode(node.label);
        map.put(head.label,head);
        for(int i = 0; i < node.neighbors.size(); i++){
            head.neighbors.add(cloneGraph(node.neighbors.get(i)));
        }
        return head;
    }
}