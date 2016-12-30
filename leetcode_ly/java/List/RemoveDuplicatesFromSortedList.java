/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
public class Solution {
    public ListNode deleteDuplicates(ListNode head) {
        if(head == null) return null; 
        ListNode slow = head; 
        ListNode fast = head.next; 
        while(fast != null){ 
            if(slow.val != fast.val){ 
                slow = slow.next; 
                slow.val = fast.val; 
            } 
            fast = fast.next; 
        } 
        slow.next = null; 
        return head; 
    }
}