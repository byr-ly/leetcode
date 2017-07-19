public class Solution {
    public List<Integer> grayCode(int n) {
        ArrayList<Integer> res = new ArrayList<>();
        if(n == 0){
            res.add(0);
            return res;
        }
        if(n == 1){
            res.add(0);
            res.add(1);
            return res;
        }
        
        //GrayCode(n)即在GrayCode(n - 1)的基础上往前倒序加上0和1
        List<Integer> temp = grayCode(n - 1);
        res = new ArrayList<>(temp);
        int addNumber = 1 << (n - 1);
        for(int i = temp.size() - 1; i >= 0; i--){
            res.add(temp.get(i) + addNumber);
        }
        return res;
    }
}