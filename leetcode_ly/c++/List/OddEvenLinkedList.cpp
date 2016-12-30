/**
 * Definition for singly-linked list.
 * struct ListNode {
 *     int val;
 *     ListNode *next;
 *     ListNode(int x) : val(x), next(NULL) {}
 * };
 */
class Solution {
public:
    ListNode* oddEvenList(ListNode* head) {
        if(!head) return NULL;
        if(!head->next) return head;
        ListNode* odd = head;
        ListNode* even = head->next;
        ListNode* node = even;
        while(even && even->next){
            odd->next = even->next;
            even->next = odd->next->next;
            odd->next->next = node;
            odd = odd->next;
            even = even->next;
        }
        return head;
    }
};