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
    TreeNode* buildTree(vector<int>& preorder, vector<int>& inorder) {
        if(preorder.size() == 0) return NULL;
        int n = preorder.size();
        return buildTree(preorder,0,n - 1,inorder,0,n - 1);
    }
    
    TreeNode* buildTree(vector<int>& preorder,int left,int right,vector<int>& inorder,int low,int high){
        if(left > right) return NULL;
        int root = preorder[left];
        int index;
        for(index = low; index <= high; index++){
            if(inorder[index] == root) break;
        }
        int leftSize = index - low;
        TreeNode* node = new TreeNode(root);
        node->left = buildTree(preorder,left + 1,left + leftSize,inorder,low,index - 1);
        node->right = buildTree(preorder,left + leftSize + 1,right,inorder,index + 1,high);
        return node;
    }
};