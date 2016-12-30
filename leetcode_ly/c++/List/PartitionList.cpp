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
    ListNode* partition(ListNode* head, int x) {
        if(!head || !head->next) return head;
        ListNode* low = new ListNode(0);
        ListNode* lowCopy = low;
        ListNode* high = new ListNode(0);
        ListNode* highCopy = high;
        ListNode* index = head;
        while(index){
            ListNode* node = index->next;
            if(index->val < x){
                low->next = index;
                low = low->next;
                low->next = NULL;
            }
            else{
                high->next = index;
                high = high->next;
                high->next = NULL;
            }
            index = node;
        }
        low->next = highCopy->next;
        return lowCopy->next;
    }
};