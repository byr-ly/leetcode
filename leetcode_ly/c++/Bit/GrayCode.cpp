class Solution {
public:
    vector<int> grayCode(int n) {
        vector<int> result;
        int num = (1 << n);
        //二进制码到格雷码：将自身右移一位与自身异或
        for(int i = 0; i < num; i++){
            result.push_back((i >> 1) ^ i);
        }
        return result;
    }
};