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
        int len1 = getLen(l1);
        int len2 = getLen(l2);
        if(len1 < len2) return addTwoNumbers(l2,l1);
        
        int flag = 0;
        ListNode temp = l1;
        ListNode ptr = l1;
        while(l1 != null && l2 != null){
            l1.val += (l2.val + flag);
            flag = 0;
            if(l1.val >= 10){
                l1.val %= 10;
                flag = 1;
            }
            temp = l1;
            l1 = l1.next;
            l2 = l2.next;
        }
        while(l1 != null && flag == 1){
            temp = l1;
            int value = l1.val + flag;
            l1.val = (l1.val + flag) % 10;
            flag = value / 10;
            l1 = l1.next;
        }
        if(l1 == null && flag == 1){
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
}