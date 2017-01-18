public class Solution {
    public int canCompleteCircuit(int[] gas, int[] cost) {
        if(gas.length == 0 || cost.length == 0 || gas.length != cost.length) return 0;
        int total = 0;
        for(int i = 0; i < gas.length; i++){
            int diff = gas[i] - cost[i];
            total += diff;
        }
        if(total < 0) return -1;
        int sum = 0;
        int start = 0;//标记起始位置
        for(int i = 0; i < gas.length; i++){
            sum += (gas[i] - cost[i]);
            if(sum < 0){
                sum = 0;
                start = i + 1;
            }
        }
        return start;
    }
}

假设从站点 i 出发，到达站点 k 之前，依然能保证油箱里油没见底儿，从k 出发后，见底儿了。
那么就说明 diff[i] + diff[i+1] + ... + diff[k] < 0，而除掉diff[k]以外，从diff[i]开始的累加都是 >= 0的。
也就是说diff[i] 也是 >= 0的，这个时候我们还有必要从站点 i + 1 尝试吗？仔细一想就知道：车要是从站点 i+1出发，
到达站点k后，甚至还没到站点k，油箱就见底儿了，因为少加了站点 i 的油。。。因此只需要从k+1站点开始即可

我们模拟一下过程：

a. 最开始，站点0是始发站，假设车开出站点p后，油箱空了，假设sum1 = diff[0] +diff[1] + ... + diff[p]，可知sum1 < 0；

b. 根据上面的论述，我们将p+1作为始发站，开出q站后，油箱又空了，设sum2 = diff[p+1] +diff[p+2] + ... + diff[q]，可知sum2 < 0。

c. 将q+1作为始发站，假设一直开到了未循环的最末站，油箱没见底儿，设sum3 = diff[q+1] +diff[q+2] + ... + diff[size-1]，可知sum3 >= 0。
要想知道车能否开回 q 站，其实就是在sum3 的基础上+sum1+sum2即可
而sum3 + sum1 + sum2 其实就是 diff数组的总和 Total，遍历完所有元素已经算出来了。因此 Total 能否 >= 0，就是是否存在这样的站点的 充分必要条件