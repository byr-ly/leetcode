public class MedianFinder {

    // Adds a number into the data structure.
    public void addNum(int num) {
        //始终保证min中的数比max中的小
        max.add(num);
        min.add(max.poll());
        if(max.size() < min.size()) max.add(min.poll());
    }

    // Returns the median of current data stream
    public double findMedian() {
        if(max.size() == min.size()) return (max.peek() + (double)min.peek()) / 2;
        else return (double)max.peek();
    }
    PriorityQueue<Integer> max = new PriorityQueue<Integer>(
        new Comparator<Integer>(){
            @Override
            public int compare(Integer a,Integer b){
                return a - b;
            }
        });
    PriorityQueue<Integer> min = new PriorityQueue<Integer>(
        new Comparator<Integer>(){
            @Override
            public int compare(Integer a,Integer b){
                return b - a;
            }
        });
};

// Your MedianFinder object will be instantiated and called as such:
// MedianFinder mf = new MedianFinder();
// mf.addNum(1);
// mf.findMedian();