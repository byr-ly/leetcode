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
    bool isPalindrome(ListNode* head) {
        if(head == NULL || head->next == NULL){
            return true;
        }
        ListNode* slow = head;
        ListNode* fast = head;
        while(fast && fast->next){
            slow = slow->next;
            fast = fast->next->next;
        }
        if(fast){
            ListNode* p = slow->next;
            slow->next = NULL;
            ListNode* q;
            while(p->next){
                q = p->next;
                p->next = slow;
                slow = p;
                p = q;
            }
            p->next = slow;
            while(p != head){
                if(p->val != head->val){
                    return false;
                }
                p = p->next;
                head = head->next;
            }
            return true;
        }
        else{
            ListNode* newNode = slow;
            if(!slow->next){
                if(head->val == slow->val) return true;
                else return false;
            }
            ListNode* p = slow->next;
            newNode->next = NULL;
            ListNode* q;
            while(p->next){
                q = p->next;
                p->next = slow;
                slow = p;
                p = q;
            }
            p->next = slow;
            while(head != newNode){
                if(head->val != p->val){
                    return false;
                }
                head = head->next;
                p = p->next;
            }
            return true;
        }
    }
};