/**
 * Definition for singly-linked list with a random pointer.
 * class RandomListNode {
 *     int label;
 *     RandomListNode next, random;
 *     RandomListNode(int x) { this.label = x; }
 * };
 */
public class Solution {
    HashMap<RandomListNode,RandomListNode> map = new HashMap<>();
    public RandomListNode copyRandomList(RandomListNode head) {
        if(head == null) return head;
        RandomListNode node = head;
        while(node != null){
            RandomListNode copy = new RandomListNode(node.label);
            map.put(node,copy);
            node = node.next;
        }
        RandomListNode ptr = head;
        while(ptr != null){
            map.get(ptr).next = map.get(ptr.next);
            map.get(ptr).random = map.get(ptr.random);
            ptr = ptr.next;
        }
        return map.get(head);
    }
}