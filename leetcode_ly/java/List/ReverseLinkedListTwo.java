/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
public class Solution {
    public ListNode reverseBetween(ListNode head, int m, int n) {
        //left->ptr1->...->right->ptr2
        if(head == null || m == n) return head;
        ListNode left = head;
        ListNode right = head;

        if(m == 1) left = null;
        while(m > 2){
            left = left.next;
            m--;
        }
        //获取要翻转的头结点
        ListNode ptr1 = head;
        if(left != null) ptr1 = left.next;
        while(n != 1){
            right = right.next;
            n--;
        }
        ListNode ptr2 = right.next;
        
        ListNode p = ptr2;
        ListNode q = ptr1;
        while(ptr1 != right){
            ptr1 = ptr1.next;
            q.next = p;
            p = q;
            q = ptr1;
        }
        ptr1.next = p;
        if(left == null) return ptr1;
        else{
            left.next = right;
            return head;
        }
    }
}