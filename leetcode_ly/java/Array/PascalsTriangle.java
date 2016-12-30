public class Solution {
    public List<List<Integer>> generate(int numRows) {
        List<List<Integer>> list = new ArrayList<>();
        if(numRows <= 0) return list;
        list.add(new ArrayList<>(Arrays.asList(1)));
        if(numRows == 1) return list;
        
        list.add(new ArrayList<>(Arrays.asList(1,1)));
        for(int i = 2; i < numRows; i++){
            ArrayList<Integer> row = new ArrayList<>();
            row.add(1);
            for(int j = 1; j < i; j++){
                row.add(list.get(i - 1).get(j - 1) + list.get(i - 1).get(j));
            }
            row.add(1);
            list.add(row);
        }
        return list;
    }
}