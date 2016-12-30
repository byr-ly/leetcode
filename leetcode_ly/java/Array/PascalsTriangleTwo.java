public class Solution {
    public List<Integer> getRow(int rowIndex) {
        List<List<Integer>> list = new ArrayList<>();
        list.add(new ArrayList<>(Arrays.asList(1)));
        //if(rowIndex <= 0) return list.get(0);
        list.add(new ArrayList<>(Arrays.asList(1,1)));
        
        for(int i = 2; i <= rowIndex; i++){
            ArrayList<Integer> temp = new ArrayList<>();
            temp.add(1);
            for(int j = 1; j < i; j++){
                temp.add(list.get(i - 1).get(j - 1) + list.get(i - 1).get(j));
            }
            temp.add(1);
            list.add(temp);
        }
        return list.get(rowIndex); 
    }
}