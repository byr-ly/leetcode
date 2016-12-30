/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
public class Solution {
    public ListNode sortList(ListNode head) {
        if(head == null || head.next == null) return head;
        ListNode slow = head;
        ListNode fast = head;
        while(fast.next != null && fast.next.next != null){
            slow = slow.next;
            fast = fast.next.next;
        }
        ListNode node = slow.next;
        slow.next = null;
        //¹é²¢ÅÅÐò
        return merge(sortList(head),sortList(node));
    }
    
    public ListNode merge(ListNode head,ListNode node){
        if(head == null) return node;
        if(node == null) return head;
        if(head.val > node.val){
            node.next = merge(head,node.next);
            return node;
        }
        else{
            head.next = merge(head.next,node);
            return head;
        }
    }
}