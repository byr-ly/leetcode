public class Solution {
    public boolean canFinish(int numCourses, int[][] prerequisites) {
    		//�����������򣬵ȼ����ж���û�л���ͳ�������Ϣ
        int[] indegree = new int[numCourses];
        for(int[] line : prerequisites){
            indegree[line[0]]++;
        }
        
        //ͳ�����Ϊ0�Ľڵ�
        Queue<Integer> q = new LinkedList<Integer>();
        for(int i = 0; i < indegree.length; i++){
            if(indegree[i] == 0) q.add(i);
        }
        
        int count = 0;
        while(!q.isEmpty()){
            int val = q.poll();
            //����ȫ��poll����û�л�
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