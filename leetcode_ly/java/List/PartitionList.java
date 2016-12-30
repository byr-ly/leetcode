/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
public class Solution {
    public ListNode partition(ListNode head, int x) {
        if(head == null) return null;
        ListNode left = new ListNode(0);
        ListNode right = new ListNode(0);
        ListNode copyLeft = left;
        ListNode copyRight = right;
        while(head != null){
            ListNode node = head.next;
            if(head.val < x){
                left.next = head;
                head.next = null;
                left = left.next;
            }
            else{
                right.next = head;
                head.next = null;
                right = right.next;
            }
            head = node;
        }
        left.next = copyRight.next;
        return copyLeft.next;
    }
}