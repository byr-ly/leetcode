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
    vector<int> result;
    
    vector<int> inorderTraversal(TreeNode* root) {
        inorder(root);
        return result;
    }
    
    void inorder(TreeNode* root){
        if(!root) return;
        inorder(root->left);
        result.push_back(root->val);
        inorder(root->right);
    }
};