/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
public class Solution {
    public ListNode swapPairs(ListNode head) {
        if(head == null) return null;
        ListNode newNode = new ListNode(0);
        ListNode node = newNode;
        node.next = head;
        ListNode ptr = head;
        while(ptr != null && ptr.next != null){
            node.next = ptr.next;
            ptr.next = ptr.next.next;
            node.next.next = ptr;
            node = node.next.next;
            ptr = ptr.next;
        }
        return newNode.next;
    }
}