/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
public class Solution {
    public boolean isPalindrome(ListNode head) {
        if(head == null || head.next == null) return true;
        ListNode slow = head;
        ListNode fast = head;
        while(fast.next != null && fast.next.next != null){
            slow = slow.next;
            fast = fast.next.next;
        }
        ListNode node = slow.next;
        slow.next = null;
        ListNode p = null;
        ListNode q = node;
        while(node.next != null){
            node = node.next;
            q.next = p;
            p = q;
            q = node;
        }
        node.next = p;
        while(head != null && node != null && head.val == node.val){
            head = head.next;
            node = node.next;
        }
        if(head == null || node == null) return true;
        else return false;
    }
}