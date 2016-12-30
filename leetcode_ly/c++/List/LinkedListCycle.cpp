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
    bool hasCycle(ListNode *head) {
        if(head == NULL || head->next == NULL){
            return false;
        }
        else{
            ListNode* front = head;
            ListNode* end = head->next;
            while(end){
                if(front == end){
                    return true;
                }
                front = front->next;
                if(end->next == NULL || end->next->next == NULL){
                    return false;
                }
                end = end->next->next;
            }
            return false;
        }
    }
};