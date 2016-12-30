/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
public class Solution {
    public ListNode addTwoNumbers(ListNode l1, ListNode l2) {
        if(l1 == null) return l2;
        if(l2 == null) return l1;
        ListNode ptr1 = reverse(l1);
        ListNode ptr2 = reverse(l2);
        ListNode node = add(ptr1,ptr2);
        ListNode result = reverse(node);
        return result;
    }
    
    public ListNode add(ListNode ptr1,ListNode ptr2){
        int len1 = getLen(ptr1);
        int len2 = getLen(ptr2);
        if(len1 < len2) return add(ptr2,ptr1);
        int flag = 0;
        ListNode ptr = ptr1;
        ListNode temp = ptr1;
        while(ptr1 != null && ptr2 != null){
            ptr1.val += (ptr2.val + flag);
            flag = 0;
            if(ptr1.val >= 10){
                ptr1.val %= 10;
                flag = 1;
            }
            temp = ptr1;
            ptr1 = ptr1.next;
            ptr2 = ptr2.next;
        }
        while(ptr1 != null && flag == 1){
            temp = ptr1;
            ptr1.val += flag;
            flag = 0;
            if(ptr1.val >= 10){
                ptr1.val %= 10;
                flag = 1;
            }
            ptr1 = ptr1.next;
        }
        if(ptr1 == null && flag == 1){
            ListNode node = new ListNode(1);
            temp.next = node;
        }
        return ptr;
    }
    
    public int getLen(ListNode head){
        int len = 0;
        while(head != null){
            len++;
            head = head.next;
        }
        return len;
    }
    
    public ListNode reverse(ListNode head){
        ListNode p = null;
        ListNode q = head;
        while(head.next != null){
            head = head.next;
            q.next = p;
            p = q;
            q = head;
        }
        head.next = p;
        return head;
    }
}