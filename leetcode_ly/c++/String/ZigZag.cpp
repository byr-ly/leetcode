class Solution {
public:
    string convert(string s, int numRows) {
        if(s.size() <= numRows || numRows == 1) return s;
        string res = "";
        int i;
        int index;
        for(i = 0; i < numRows; i++){
            index = i;
            res += s[index];
            
            for(int k = 1; index < s.size(); k++){
                if(i == 0 || i == numRows - 1){
                    index = index + 2 * numRows - 2;
                }
                else{
                    if(k % 2 != 0){
                        index = index + (numRows - i - 1) * 2;
                    }
                    else{
                        index = index + i * 2;
                    }
                }
                
                if(index < s.size()){
                    res += s[index];
                }
            }
        }
        return res;
    }
};