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
    
    vector<int> preorderTraversal(TreeNode* root) {
        preorder(root);
        return result;
    }
    
    void preorder(TreeNode* root){
        if(!root) return;
        result.push_back(root->val);
        preorder(root->left);
        preorder(root->right);
    }
};