class Solution {
public:
    vector<int> grayCode(int n) {
        vector<int> result;
        int num = (1 << n);
        //�������뵽�����룺����������һλ���������
        for(int i = 0; i < num; i++){
            result.push_back((i >> 1) ^ i);
        }
        return result;
    }
};