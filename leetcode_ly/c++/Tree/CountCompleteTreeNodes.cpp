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
    int countNodes(TreeNode* root) {
        if(!root) return 0;
        int l = getLeft(root);
        int r = getRight(root);
        
        if(l == r) return (2 << (l - 1)) - 1;
        else return countNodes(root->left) + countNodes(root->right) + 1;
    }
    
    int getLeft(TreeNode* root){
        int cnt = 0;
        while(root){
            cnt++;
            root = root->left;
        }
        return cnt;
    }
    
    int getRight(TreeNode* root){
        int cnt = 0;
        while(root){
            cnt++;
            root = root->right;
        }
        return cnt;
    }
};