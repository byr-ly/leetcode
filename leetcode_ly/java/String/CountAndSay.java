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

//java��'1' - '0' = 1 ���ǲ���ʹ��1 + '0'
//java��String��ĺ�������������String�Ļ����븳ֵ������������ԭ����String����� ����res = res.trim()