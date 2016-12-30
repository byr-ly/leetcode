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
    bool isSymmetric(TreeNode* root) {
        if(!root) return true;
        return isMirror(root->left,root->right);
    }
    
    bool isMirror(TreeNode* t1, TreeNode* t2){
        if(!t1) return t2 == NULL;
        if(!t2) return t1 == NULL;
        if(t1->val != t2->val) return false;
        return isMirror(t1->right,t2->left) && isMirror(t1->left,t2->right);
    }
};