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
    TreeNode* buildTree(vector<int>& inorder, vector<int>& postorder) {
        int n = inorder.size();
        if(n == 0) return NULL;
        return getResult(inorder,0,n - 1,postorder,0,n - 1);
    }
    
    TreeNode* getResult(vector<int>& inorder,int left,int right,vector<int>& postorder,int low,int high){
        if(left > right) return NULL;
        int root = postorder[high];
        int index;
        for(index = left;index <= right;index++){
            if(inorder[index] == root) break;
            else continue;
        }
        int leftSize = index - left;
        TreeNode* node = new TreeNode(root);
        node->left = getResult(inorder,left,index - 1,postorder,low,low + leftSize - 1);
        node->right = getResult(inorder,index + 1,right,postorder,low + leftSize,high - 1);
        return node;
    }
};