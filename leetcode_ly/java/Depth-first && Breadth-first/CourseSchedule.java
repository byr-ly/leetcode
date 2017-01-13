public class Solution {
    public boolean canFinish(int numCourses, int[][] prerequisites) {
    		//利用拓扑排序，等价于判断有没有环，统计入度信息
        int[] indegree = new int[numCourses];
        for(int[] line : prerequisites){
            indegree[line[0]]++;
        }
        
        //统计入度为0的节点
        Queue<Integer> q = new LinkedList<Integer>();
        for(int i = 0; i < indegree.length; i++){
            if(indegree[i] == 0) q.add(i);
        }
        
        int count = 0;
        while(!q.isEmpty()){
            int val = q.poll();
            //若能全部poll，则没有环
            count++;
            for(int i = 0; i < prerequisites.length; i++){
                if(val == prerequisites[i][1]){
                    indegree[prerequisites[i][0]]--;
                    if(indegree[prerequisites[i][0]] == 0) q.add(prerequisites[i][0]);
                }
            }
        }
        return count == numCourses;
    }
}