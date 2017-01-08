public class Solution {
	//注意考虑负数和越界问题，递归解法击败20%的人
    public int integerReplacement(int n) {
        if(n <= 0) return -1;
        if(n == Integer.MAX_VALUE) return 32;
        if(n == 1) return 0;
        if(n % 2 == 0) return 1 + integerReplacement(n / 2);
        else return 1 + Math.min(integerReplacement(n + 1),integerReplacement(n - 1));
    }
}

//bit manipulation击败40%的人
public class Solution {
    public int integerReplacement(int n) {
        int cnt = 0;
        while(n != 1){
            if((n & 1) == 0) n = n >>> 1;
            //bit 1的数量越少越容易变成1，注意3为特殊情况只能向下取
            else if(n == 3 || getBits(n - 1) < getBits(n + 1)) n = n - 1;
            else n = n + 1;
            cnt++;
        }
        return cnt;
    }
    
    public int getBits(int n){
        int cnt = 0;
        while(n != 0){
            n = (n & (n - 1));
            cnt++;
        }
        return cnt;
    }
}