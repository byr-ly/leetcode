/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
public class Solution {
    public ListNode rotateRight(ListNode head, int k){
        if(head == null || k == 0) return head;
        int len = 0;
        ListNode node = head;
        while(node != null){
            node = node.next;
            len++;
        }
        k = k % len;
        if(k == 0) return head;
        
        ListNode slow = head;
        ListNode fast = head;
        while(k != 0){
            fast = fast.next;
            k--;
        }
        while(fast.next != null){
            slow = slow.next;
            fast = fast.next;
        }
        ListNode ptr = slow.next;
        slow.next = null;
        fast.next = head;
        return ptr;
    }
}