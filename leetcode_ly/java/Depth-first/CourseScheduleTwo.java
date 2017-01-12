public class Solution {
    public int[] findOrder(int numCourses, int[][] prerequisites) {
        int[] res = new int[numCourses];
        ArrayList<Integer> list = new ArrayList<Integer>();
        //若没有先决条件，则按顺序学下来
        if(prerequisites == null || prerequisites.length == 0){
            for(int i = 0; i < numCourses; i++){
                res[i] = i;
            }
            return res;
        }
        int[] indegree = new int[numCourses];
        for(int[] line : prerequisites){
            indegree[line[0]]++;
        }
        
        Queue<Integer> q = new LinkedList<Integer>();
        for(int i = 0; i < indegree.length; i++){
            if(indegree[i] == 0){
                q.add(i);
                list.add(i);
            }
        }
        
        int count = 0;
        while(!q.isEmpty()){
            int val = q.poll();
            count++;
            for(int i = 0; i < prerequisites.length; i++){
                if(val == prerequisites[i][1]){
                    indegree[prerequisites[i][0]]--;
                    if(indegree[prerequisites[i][0]] == 0){
                        q.add(prerequisites[i][0]);
                        list.add(prerequisites[i][0]);
                    }
                }
            }
        }
        
        //若没有环则可以完成学习
        if(count == numCourses){
            for(int i = 0; i < list.size(); i++){
                res[i] = list.get(i);
            }
            return res;
        }
        //若有环则返回
        return new int[0];
    }
}