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
        if(head == null || head.next == null) return head;
        ListNode ptr = new ListNode(0);
        ptr.next = head;
        ListNode copy = ptr;
        ListNode pre = head;
        ListNode pos = head.next;
        while(pos != null){
            if(pos.val != pre.val){
                ptr = ptr.next;
                pre = pre.next;
                pos = pos.next;
            }
            else{
                while(pos != null && pos.val == pre.val){
                    pos = pos.next;
                }
                ptr.next = pos;
                if(pos != null){
                    pre = pos;
                    pos = pos.next;
                }
            }
        }
        //ptr.next = null;
        return copy.next;
    }
}