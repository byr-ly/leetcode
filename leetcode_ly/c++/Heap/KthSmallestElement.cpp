class Solution {
public:
    int kthSmallest(vector<vector<int>>& matrix, int k) {
        priority_queue<int, vector<int>,less<int>> q;
        for (int i = 0; i < matrix.size(); i++) {
            for (int j = 0; j < matrix[i].size(); j++) {
                q.push(matrix[i][j]);
                if (q.size() > k) q.pop();
            }
        }
        return q.top();
    }
};

class Solution {
public:
    int kthSmallest(vector<vector<int>>& matrix, int k) {
        priority_queue<int,vector<int>,greater<int>> q;
        for(int i = 0; i < matrix.size(); i++){
            for(int j = 0; j < matrix[0].size(); j++){
                q.push(matrix[i][j]);
            }
        }
        for(int i = 0; i < k - 1; i++){
            q.pop();
        }
        return q.top();
    }
};