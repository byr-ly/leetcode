public class Solution {
    public boolean canPlaceFlowers(int[] flowerbed, int n) {
        if(flowerbed == null || flowerbed.length == 0) return false;
        int res = 0;
        int count = 1;//开头有两个0就可以放一个1
        for(int i = 0; i < flowerbed.length; i++){
            if(flowerbed[i] == 0){
                count++;
            }
            else{
                res += (count - 1) / 2;
                count = 0;
            }
        }
        //末尾有两个0或者flowerbed.length <= 2
        if(count != 0) res += (count / 2);
        return res >= n;
    }
}