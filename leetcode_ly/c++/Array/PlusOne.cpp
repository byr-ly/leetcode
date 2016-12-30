class Solution {
public:
    vector<int> plusOne(vector<int>& digits) {
        int len = digits.size();
        if(digits[len - 1] != 9){
            digits[len - 1] += 1;
        }
        else{
            int i = 1;
            while((len - i >= 0) && (digits[len - i] == 9)){
                digits[len - i] = 0;
                i++;
            }
            if(i != (len + 1)){
                digits[len - i] += 1;
            }
            else{
                digits.insert(digits.begin(),1);
            }
            
        }
        return digits;
    }
};