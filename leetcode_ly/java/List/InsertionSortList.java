/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
public class Solution {
    public ListNode insertionSortList(ListNode head) {
        if(head == null) return null;
        ListNode node = new ListNode(0);
        while(head != null){
            ListNode pre = node;
            while(pre.next != null && pre.next.val <= head.val){
                pre = pre.next;
            }
            ListNode temp = head.next;
            head.next = pre.next;
            pre.next = head;
            head = temp;
        }
        return node.next;
    }
}