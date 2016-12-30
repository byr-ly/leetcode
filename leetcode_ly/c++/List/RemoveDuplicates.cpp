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
    ListNode* deleteDuplicates(ListNode* head) {
        if(head == NULL || head->next == NULL){
            return head;
        }
        else{
            ListNode* front = head;
            ListNode* back = head->next;
            while(back != NULL){
                if(head->val == back->val){
                    back = back->next;
                    head->next = back;
                }
                else{
                    head = back;
                    back = head->next;
                }
            }
            return front;
        }
    }
};