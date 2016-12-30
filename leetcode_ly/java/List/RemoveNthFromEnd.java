/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
public class Solution {
    public ListNode removeNthFromEnd(ListNode head, int n) {
        if(head == null || head.next == null) return null;
        int len = 0;
        ListNode node = head;
        while(node != null){
            node = node.next;
            len++;
        }
        if(len == n){
            head = head.next;
            return head;
        }
        ListNode low = head;
        ListNode high = head;
        while(n > 0 && high.next != null){
            high = high.next;
            n--;
        }
        while(high.next != null){
            high = high.next;
            low = low.next;
        }
        low.next = low.next.next;
        return head;
    }
}