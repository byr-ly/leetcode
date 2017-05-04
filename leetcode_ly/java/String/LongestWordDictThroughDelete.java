public class Solution {
    public String findLongestWord(String s, List<String> d) {
        String longest = "";
        for(String dict : d){
            int i = 0;
            int j = 0;
            while(i < dict.length() && j < s.length()){
                if(dict.charAt(i) == s.charAt(j)){
                    i++;
                }
                j++;
            }
            if(i == dict.length()){
                if(dict.length() > longest.length() || (dict.length() == longest.length() && dict.compareTo(longest) < 0)){
                    longest = dict;
                }
            }
        }
        return longest;
    }
}