/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
public class Solution {
    public void deleteNode(ListNode node) {
        ListNode back; 
        while(node.next != null){ 
            back = node.next; 
            node.val = back.val; 
            if(back.next == null) break; 
            node = node.next; 
        } 
        node.next = null; 
    }
}