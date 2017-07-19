public class Solution {
    public boolean isHappy(int n) {
        HashSet<Integer> set = new HashSet<>();
        int res = 0;
        int remain = 0;
        while(!set.contains(n)){
            set.add(n);
            while(n != 0){
                remain = n % 10;
                res += remain * remain;
                n /= 10;
            }
            if(res == 1) return true;
            else{
                n = res;
                res = 0;
            }
        }
        return false;
    }
}