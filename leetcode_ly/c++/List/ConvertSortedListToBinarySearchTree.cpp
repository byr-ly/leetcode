/**
 * Definition for singly-linked list.
 * struct ListNode {
 *     int val;
 *     ListNode *next;
 *     ListNode(int x) : val(x), next(NULL) {}
 * };
 */
/**
 * Definition for a binary tree node.
 * struct TreeNode {
 *     int val;
 *     TreeNode *left;
 *     TreeNode *right;
 *     TreeNode(int x) : val(x), left(NULL), right(NULL) {}
 * };
 */
class Solution {
public:
    TreeNode* sortedListToBST(ListNode* head) {
        return buildTree(head,NULL);
    }
    
    TreeNode* buildTree(ListNode* head,ListNode* end){
        if(head == end) return NULL;
        ListNode* low = head;
        ListNode* high = head;
        while(high->next != end && high->next->next != end){
            low = low->next;
            high = high->next->next;
        }
        TreeNode* root = new TreeNode(low->val);
        root->left = buildTree(head,low);
        root->right = buildTree(low->next,end);
        return root;
    }
};