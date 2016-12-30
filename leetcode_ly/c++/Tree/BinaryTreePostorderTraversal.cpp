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

    vector<int> postorderTraversal(TreeNode* root) {
        postorder(root);
        return result;
    }
    
    void postorder(TreeNode* root){
        if(!root) return;
        postorder(root->left);
        postorder(root->right);
        result.push_back(root->val);
    }
};