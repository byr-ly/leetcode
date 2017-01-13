public class Solution {
    public int[] findOrder(int numCourses, int[][] prerequisites) {
        int[] res = new int[numCourses];
        ArrayList<Integer> list = new ArrayList<Integer>();
        //��û���Ⱦ���������˳��ѧ����
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
        
        //��û�л���������ѧϰ
        if(count == numCourses){
            for(int i = 0; i < list.size(); i++){
                res[i] = list.get(i);
            }
            return res;
        }
        //���л��򷵻�
        return new int[0];
    }
}