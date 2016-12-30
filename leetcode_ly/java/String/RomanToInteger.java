public class Solution {
    public int romanToInt(String s) {
        if(s.isEmpty()) return 0;
        char cur = s.charAt(0);;
        int i = 0;
        int res = 0;
        while(i < s.length()){
            switch(s.charAt(i)){
                case 'I':
                    res += 1;
                    break;
                case 'V':
                    if(cur == 'I') res += 3;
                    else res += 5;
                    break;
                case 'X':
                    if(cur == 'I') res += 8;
                    else res += 10;
                    break;
                case 'L':
                    if(cur == 'X') res += 30;
                    else res += 50;
                    break;
                case 'C':
                    if(cur == 'X') res += 80;
                    else res += 100;
                    break;
                case 'D':
                    if(cur == 'C') res += 300;
                    else res += 500;
                    break;
                case 'M':
                    if(cur == 'C') res += 800;
                    else res += 1000;
                    break;
                default:
                    break;
            }
            cur = s.charAt(i);
            i++;
        }
        return res;
    }
}