public class Solution {
    public int lengthOfLongestSubstring(String s) {
        if(s == null || s.isEmpty()) return 0;
        HashMap<Character,Integer> map = new HashMap<Character,Integer>();
        //i��ʾ�ַ����յ㣬j��ʾ�´β��ҵ���㣬ֻ��Խ��Խ��
        int j = 0;
        int max = 0;
        for(int i = 0; i < s.length(); i++){
            if(map.containsKey(s.charAt(i))){
                j = Math.max(j,map.get(s.charAt(i)) + 1);
            }
            map.put(s.charAt(i),i);
            max = Math.max(max,i - j + 1);
        }
        return max;
    }
}