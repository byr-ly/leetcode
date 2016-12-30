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
    ListNode* swapPairs(ListNode* head) {
        if(head == NULL || head->next == NULL){
            return head;
        }
        else{
            ListNode* front = head;
            ListNode* end = head->next;
            do{
                int temp = front->val;
                front->val = end->val;
                end->val = temp;
                if(front->next->next == NULL || end->next->next == NULL){
                    break;
                }
                front = front->next->next;
                end = end->next->next;
            }while(front != NULL && end != NULL);
            return head;
        }
    }
};