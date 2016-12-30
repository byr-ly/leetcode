class Solution {
public:
    int computeArea(int A, int B, int C, int D, int E, int F, int G, int H) {
        int result;
        if(F >= D || H <= B || E >= C || G <= A){
            result = (C - A) * (D - B) + (G - E) * (H - F);
        }
        else{
            int a = (A < E) ? E : A;
            int b = (B < F) ? F : B;
            int c = (C < G) ? C : G;
            int d = (D < H) ? D : H;
            result = (C - A) * (D - B) + (G - E) * (H - F) - (c - a) * (d - b);
        }
        return result;
    }
};