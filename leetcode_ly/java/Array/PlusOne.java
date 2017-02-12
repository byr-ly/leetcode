public class Solution {
    public int[] plusOne(int[] digits) {
        if(!isNine(digits)){
            int[] res = new int[digits.length];
            int flag = 1;
            int index = res.length - 1;
            while(index >= 0 && digits[index] == 9){
                res[index] = 0;
                index--;
            }
            while(index >= 0){
                res[index] = digits[index] + flag;
                flag = 0;
                index--;
            }
            return res;
        }
        else{
            int[] res = new int[digits.length + 1];
            res[0] = 1;
            for(int i = 1; i < res.length; i++){
                res[i] = 0;
            }
            return res;
        }
    }
    
    boolean isNine(int[] digits){
        int n = digits.length;
        for(int i = 0; i < n; i++){
            if(digits[i] != 9) return false;
        }
        return true;
    }
}

public class Solution {
    public int[] plusOne(int[] digits) {
        if(digits == null) return new int[0];
        int sum = digits[digits.length - 1] + 1;
        digits[digits.length - 1] = sum % 10;
        int remain = sum / 10;
        for(int i = digits.length - 2; i >= 0; i--){
            sum = digits[i] + remain;
            digits[i] = sum % 10;
            remain = sum / 10;
        }
        if(remain == 1){
            int[] res = new int[digits.length + 1];
            Arrays.fill(res,0);
            res[0] = 1;
            return res;
        }
        return digits;
    }
}