public class NumArray {
    
    class segmentNode{
        int start;
        int end;
        segmentNode left;
        segmentNode right;
        int sum;
    
        public segmentNode(int start,int end){
            this.start = start;
            this.end = end;
            this.left = null;
            this.right = null;
            this.sum = 0;
        }
    }
    
    segmentNode root = null;

    public NumArray(int[] nums) {
        root = buildTree(nums,0,nums.length - 1);
    }
    
    public segmentNode buildTree(int[] nums,int start,int end){
        if(start > end) return null;
        else{
            segmentNode node = new segmentNode(start,end);
            if(start == end) node.sum = nums[start];
            else{
                int middle = start + (end - start) / 2;
                node.left = buildTree(nums,start,middle);
                node.right = buildTree(nums,middle + 1,end);
                node.sum = node.left.sum + node.right.sum;
            }
            return node;
        }
    }

    void update(int i, int val) {
        update(i,root,val);
    }
    
    public void update(int i,segmentNode root,int val){
        if(i == root.start && i == root.end) root.sum = val;
        else{
            int middle = root.start + (root.end - root.start) / 2;
            if(i <= middle){
                update(i,root.left,val);
            }
            else{
                update(i,root.right,val);
            }
            root.sum = root.left.sum + root.right.sum;
        }
    }

    public int sumRange(int i, int j) {
        return sumRange(i,j,root);
    }
    
    public int sumRange(int i,int j,segmentNode root){
        if(i == root.start && j == root.end) return root.sum;
        else{
            int middle = root.start + (root.end - root.start) / 2;
            if(i > middle) return sumRange(i,j,root.right);
            else if(j <= middle) return sumRange(i,j,root.left);
            else return sumRange(i,middle,root.left) + sumRange(middle + 1,j,root.right);
        }
    }
}


// Your NumArray object will be instantiated and called as such:
// NumArray numArray = new NumArray(nums);
// numArray.sumRange(0, 1);
// numArray.update(1, 10);
// numArray.sumRange(1, 2);