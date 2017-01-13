public class Solution {
    public List<String> findAllConcatenatedWordsInADict(String[] words) {
        List<String> res = new ArrayList<String>();
        HashSet<String> dict = new HashSet<String>();
        //按字符串长度排序
        Arrays.sort(words,new Comparator<String>(){
            @Override
            public int compare(String a,String b){
                return a.length() - b.length();
            }
        });
        
        for(int i = 0; i < words.length; i++){
            if(canForm(words[i],dict)){
                res.add(words[i]);
            }
            //将可能的短词放入字典
            dict.add(words[i]);
        }
        return res;
    }
    
    public boolean canForm(String s,HashSet<String> dict){
        if(dict.isEmpty()) return false;
        //标记当前位置的字符前面的字符串是不是短词
        boolean[] dp = new boolean[s.length() + 1];
        dp[0] = true;
        
        for(int i = 1; i <= s.length(); i++){
            for(int j = 0; j < i; j++){
                //使搜索从上一个短词结束开始
                if(!dp[j]) continue;
                if(dict.contains(s.substring(j,i))){
                    dp[i] = true;
                    break;
                }
            }
        }
        return dp[s.length()];
    }
}