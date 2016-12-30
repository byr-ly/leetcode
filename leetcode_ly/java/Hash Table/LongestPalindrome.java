public class Solution {
    public int longestPalindrome(String s) {
        HashMap<Character,Integer> map = new HashMap<Character,Integer>();
        for(int i = 0; i < s.length(); i++){
            char c = s.charAt(i);
            if(map.containsKey(c)) map.put(c,map.get(c) + 1);
            else map.put(c,1);
        }
        int res = 0;
        boolean flag = false;
        for(int val : map.values()){
            if(val % 2 == 0) res += val;
            else{
                flag = true;
                res += (val - 1);
            }
        }
        return (flag == true) ? res + 1 : res;
    }
}