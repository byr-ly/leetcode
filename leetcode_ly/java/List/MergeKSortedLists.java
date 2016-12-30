/**
 * Definition for singly-linked list.
 * public class ListNode {
 *     int val;
 *     ListNode next;
 *     ListNode(int x) { val = x; }
 * }
 */
public class Solution {
    public ListNode mergeKLists(ListNode[] lists) {
        int len = lists.length;
        if(len == 0) return null;
        return helper(lists,0,len - 1);
    }
    
    public ListNode helper(ListNode[] lists,int l,int r){
        if(l < r){
            int m = l + (r - l) / 2;
            return merge(helper(lists,l,m),helper(lists,m + 1,r));
        }
        return lists[l];
    }
    
    public ListNode merge(ListNode l1,ListNode l2){
        if(l1 == null) return l2;
        if(l2 == null) return l1;
        if(l1.val < l2.val){
            l1.next = merge(l1.next,l2);
            return l1;
        }
        else{
            l2.next = merge(l1,l2.next);
            return l2;
        }
    }
}