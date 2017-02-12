/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
public class Solution {
    public ListNode removeElements(ListNode head, int val) {
        ListNode pre = new ListNode(1);
        pre.next = head;
        ListNode slow = pre;
        ListNode fast = head;
        while(fast != null){
            if(fast.val != val){
                slow.next = fast;
                slow = slow.next;
            }
            fast = fast.next;
        }
        slow.next = null;
        return pre.next;
    }
}