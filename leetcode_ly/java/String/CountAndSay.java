public class Solution {
    public String countAndSay(int n) {
        if(n == 1) return "1";
        String front = countAndSay(n - 1);
        
        int i = 0;
        int j = 0;
        String res = "";
        int count = 0;
        
        while(j < front.length()){
            while(j < front.length() && front.charAt(i) == front.charAt(j)){
                count++;
                j++;
            }
            res = res.concat(String.valueOf(count));
            res += (front.charAt(i));
            count = 0;
            i = j;
        }
        return res;
    }
}

//java中'1' - '0' = 1 但是不能使用1 + '0'
//java中String里的函数返回类型是String的话必须赋值给变量，否则原来的String不会变 比如res = res.trim()