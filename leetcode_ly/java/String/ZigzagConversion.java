public class Solution {
    public String convert(String s, int numRows) {
        if(s.length() <= numRows || numRows <= 1) return s;
        StringBuffer res = new StringBuffer();
        
        //i��ʾ����
        for(int i = 0; i < numRows; i++){
            //index��ʾҪ�����ַ����±�
            int index = i;
            res.append(s.charAt(index));
            
            //k��ʾһ���еĵڼ�����
            for(int k = 1; index < s.length(); k++){
                if(i == 0 || i == numRows - 1){
                    index = index + numRows * 2 - 2;
                }
                else{
                    if(k % 2 != 0){
                        index = index + (numRows - 1 - i) * 2;
                    }
                    else{
                        index = index + i * 2;
                    }
                }
                
                if(index < s.length()) res.append(s.charAt(index));
            }
        }
        return res.toString();
    }
}