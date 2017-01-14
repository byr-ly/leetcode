public class Solution {
    public List<Integer> findMinHeightTrees(int n, int[][] edges) {
        List<Integer> res = new ArrayList<Integer>();
        if(n == 1){
            res.add(0);
            return res;
        }
        
        //将图的结构保存起来（谁和谁相连）
        List<HashSet<Integer>> indegree = new ArrayList<HashSet<Integer>>();
        for(int i = 0; i < n; i++){
            indegree.add(new HashSet<Integer>());
        }
        for(int[] edge : edges){
            indegree.get(edge[0]).add(edge[1]);
            indegree.get(edge[1]).add(edge[0]);
        }
        //找出初始的叶子节点
        List<Integer> leaves = new ArrayList<Integer>();
        for(int i = 0; i < n; i++){
            if(indegree.get(i).size() == 1) leaves.add(i);
        }
        
        while(n > 2){
            //循环将叶子节点删掉，将新的叶子节点存入结果中
            n -= leaves.size();
            ArrayList<Integer> newLeaves = new ArrayList<Integer>();
            for(int i : leaves){
                //HashSet取元素
                int j = indegree.get(i).iterator().next();
                indegree.get(j).remove(i);
                if(indegree.get(j).size() == 1) newLeaves.add(j);
            }
            leaves = newLeaves;
        }
        return leaves;
    }
}