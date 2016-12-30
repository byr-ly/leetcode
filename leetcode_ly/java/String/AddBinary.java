public class Solution {
    public String addBinary(String a, String b) {
        int la = a.length();
        int lb = b.length();
        if(la < lb) return addBinary(b,a);
        
        StringBuffer res = new StringBuffer(a);
        int flag = 0;
        for(int i = lb - 1; i >= 0; i--){
            int ia = Integer.parseInt(String.valueOf(a.charAt(i + la - lb)));
            int ib = Integer.parseInt(String.valueOf(b.charAt(i)));
            if(ia + ib + flag == 2){
                flag = 1;
                res.setCharAt(i + la - lb,'0');
            }
            else if(ia + ib + flag == 3){
                flag = 1;
                res.setCharAt(i + la - lb,'1');
            }
            else if(ia + ib + flag == 0){
                flag = 0;
                res.setCharAt(i + la - lb,'0');
            }
            else{
                flag = 0;
                res.setCharAt(i + la - lb,'1');
            }
        }
        if(la >= lb && flag == 0) return res.toString();
        else if(la > lb && flag == 1){
            int index = la - lb - 1;
            while(index >= 0 && flag == 1){
                int val = Integer.parseInt(String.valueOf(a.charAt(index)));
                if(val + flag == 2){
                    flag = 1;
                    res.setCharAt(index,'0');
                }
                else if(val + flag == 3){
                    flag = 1;
                    res.setCharAt(index,'1');
                }
                else if(val + flag == 0){
                    flag = 0;
                    res.setCharAt(index,'0');
                }
                else {
                    flag = 0;
                    res.setCharAt(index,'1');
                }
                index--;
            }
            if(index == -1 && flag == 1) return new StringBuffer("1").append(res).toString();
        }
        else return new StringBuffer("1").append(res).toString();
        return res.toString();
    }
}



public class Solution {
    public String addBinary(String a, String b) {
        StringBuilder sb = new StringBuilder();
        int i = a.length() - 1, j = b.length() -1, carry = 0;
        while (i >= 0 || j >= 0) {
            int sum = carry;
            if (j >= 0) sum += b.charAt(j--) - '0';
            if (i >= 0) sum += a.charAt(i--) - '0';
            sb.append(sum % 2);
            carry = sum / 2;
        }
        if (carry != 0) sb.append(carry);
        return sb.reverse().toString();
    }
}