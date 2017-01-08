//用抽屉法判断是否存在相同字符，击败8%的人
public class Solution {
    public int maxProduct(String[] words) {
        int max = Integer.MIN_VALUE;
        for(int i = 0; i < words.length - 1; i++){
            for(int j = i + 1; j < words.length; j++){
                if(isValid(words[i],words[j])){
                    max = Math.max(max,words[i].length() * words[j].length());
                }
            }
        }
        return max == Integer.MIN_VALUE ? 0 : max;
    }
    
    public boolean isValid(String a,String b){
        int[] pos = new int[26];
        for(int i = 0; i < a.length(); i++){
            pos[a.charAt(i) - 'a']++;
        }
        for(int i = 0; i < b.length(); i++){
            if(pos[b.charAt(i) - 'a'] > 0) return false;
        }
        return true;
    }
}


//利用bit manipulation击败80%的人
public class Solution {
    public int maxProduct(String[] words) {
        int max = Integer.MIN_VALUE;
        int[] val = new int[words.length];
        for(int i = 0; i < words.length; i++){
            for(int j = 0; j < words[i].length(); j++){
                val[i] |= (1 << (words[i].charAt(j) - 'a'));
            }
        }
        
        for(int i = 0; i < val.length - 1; i++){
            for(int j = i + 1; j < val.length; j++){
                if((val[i] & val[j]) == 0){
                    max = Math.max(max,words[i].length() * words[j].length());
                }
            }
        }
        return max == Integer.MIN_VALUE ? 0 : max;
    }
}