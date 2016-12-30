public class Solution {
    public String convert(String s, int numRows) {
        if(s.length() <= numRows || numRows <= 1) return s;
        StringBuffer res = new StringBuffer();
        
        //i表示行数
        for(int i = 0; i < numRows; i++){
            //index表示要连接字符的下标
            int index = i;
            res.append(s.charAt(index));
            
            //k表示一行中的第几个数
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