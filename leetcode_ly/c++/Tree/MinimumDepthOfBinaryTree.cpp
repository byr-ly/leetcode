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
    int minDepth(TreeNode* root) {
        if(!root) return 0;
        else if(root->left && root->right){
            int left = minDepth(root->left);
            int right = minDepth(root->right);
            return (left < right) ? (1 + left) : (1 + right);
        }
        else{
            int left = minDepth(root->left);
            int right = minDepth(root->right);
            return (left > right) ? (1 + left) : (1 + right);
        }
    }
};