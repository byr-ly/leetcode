public class Solution {
    public List<String> findItinerary(String[][] tickets) {
        List<String> res = new ArrayList<String>();
        if(tickets == null || tickets.length == 0) return res;
        HashMap<String,PriorityQueue<String>> map = new HashMap<String,PriorityQueue<String>>();
        for(int i = 0; i < tickets.length; i++){
            if(map.containsKey(tickets[i][0])){
                PriorityQueue<String> q = map.get(tickets[i][0]);
                q.add(tickets[i][1]);
                map.put(tickets[i][0],q);
            }
            else{
                PriorityQueue<String> q = new PriorityQueue<String>(
                    new Comparator<String>(){
                        @Override
                        public int compare(String a,String b){
                            if(a.compareTo(b) < 0) return -1;
                            else return 1;
                        }
                    });
                q.add(tickets[i][1]);
                map.put(tickets[i][0],q);
            }
        }
        visit(res,map,"JFK");
        return res;
    }
    
    public void visit(List<String> res,HashMap<String,PriorityQueue<String>> map,String s){
        PriorityQueue<String> q = map.get(s);
        while(q != null && !q.isEmpty()){
            visit(res,map,q.poll());
        }
        //从后往前添加，会把堵塞的路径率先添加到结果里
        res.add(0,s);
    }
}