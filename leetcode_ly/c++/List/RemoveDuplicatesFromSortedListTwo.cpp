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
        if(!head || !head->next) return head;
        ListNode* node = new ListNode(-1);
        node->next = head;
        ListNode* low = node;
        ListNode* high = head;
        while(high && high->next){
            if(high->val == high->next->val){
                int value = high->val;
                while(high->val == value){
                    high = high->next;
                    if(!high) break;
                }
                low->next = high;
            }
            else{
                high = high->next;
                low = low->next;
            }
        }
        return node->next;
    }
};