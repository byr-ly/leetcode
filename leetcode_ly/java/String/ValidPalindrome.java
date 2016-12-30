public class Solution {
    public boolean isPalindrome(String s) {
        String res = s.replaceAll("[^A-Za-z0-9]","").toLowerCase();
        String result = new StringBuffer(res).reverse().toString();
        return res.equals(result);
    }
}