/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
public class Solution {
    public ListNode reverseList(ListNode head) {
        if(head == null) return head; 
        ListNode p = null; 
        ListNode q = head; 
        while(head.next != null){ 
            head = head.next; 
            q.next = p; 
            p = q; 
            q = head; 
        } 
        head.next = p; 
        return head; 
    }
}