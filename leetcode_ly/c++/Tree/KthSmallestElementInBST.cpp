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
    vector<int> ret;
    
    int kthSmallest(TreeNode* root, int k) {
        inorder(root);
        return ret[k - 1];
    }
    
    void inorder(TreeNode* root){
        if(!root) return;
        inorder(root->left);
        ret.push_back(root->val);
        inorder(root->right);
    }
};