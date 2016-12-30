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
    ListNode* removeElements(ListNode* head, int val) {
        ListNode* start = new ListNode(0);
        start->next = head;
        ListNode* p = start;
        ListNode* q = head;
        while(q){
            if(q->val == val){
                p->next = q->next;
            }
            else{
                p = p->next;
            }
            q = q->next;
        }
        return start->next;
    }
};