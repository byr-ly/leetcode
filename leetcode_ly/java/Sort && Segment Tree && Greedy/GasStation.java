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
        int start = 0;//�����ʼλ��
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

�����վ�� i ����������վ�� k ֮ǰ����Ȼ�ܱ�֤��������û���׶�����k �����󣬼��׶��ˡ�
��ô��˵�� diff[i] + diff[i+1] + ... + diff[k] < 0��������diff[k]���⣬��diff[i]��ʼ���ۼӶ��� >= 0�ġ�
Ҳ����˵diff[i] Ҳ�� >= 0�ģ����ʱ�����ǻ��б�Ҫ��վ�� i + 1 ��������ϸһ���֪������Ҫ�Ǵ�վ�� i+1������
����վ��k��������û��վ��k������ͼ��׶��ˣ���Ϊ�ټ���վ�� i ���͡��������ֻ��Ҫ��k+1վ�㿪ʼ����

����ģ��һ�¹��̣�

a. �ʼ��վ��0��ʼ��վ�����賵����վ��p��������ˣ�����sum1 = diff[0] +diff[1] + ... + diff[p]����֪sum1 < 0��

b. ������������������ǽ�p+1��Ϊʼ��վ������qվ�������ֿ��ˣ���sum2 = diff[p+1] +diff[p+2] + ... + diff[q]����֪sum2 < 0��

c. ��q+1��Ϊʼ��վ������һֱ������δѭ������ĩվ������û���׶�����sum3 = diff[q+1] +diff[q+2] + ... + diff[size-1]����֪sum3 >= 0��
Ҫ��֪�����ܷ񿪻� q վ����ʵ������sum3 �Ļ�����+sum1+sum2����
��sum3 + sum1 + sum2 ��ʵ���� diff������ܺ� Total������������Ԫ���Ѿ�������ˡ���� Total �ܷ� >= 0�������Ƿ����������վ��� ��ֱ�Ҫ����