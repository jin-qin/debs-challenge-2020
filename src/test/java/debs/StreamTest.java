package debs;

import org.junit.Test;

public class StreamTest {

        // public int count1 = 0;

        // public int count2 = 0;
        // @Test
        // public void threadTest(){
        //     Thread th1 = new Thread(){
        //         @Override
        //         public void run() {
        //             while (true){
        //                 try {
        //                     Thread.sleep(1);
        //                 } catch (InterruptedException e) {
        //                     e.printStackTrace();
        //                 }
        //                 count1 += 1;
        //             }
        //         }
        //     };
        //     Thread th2 = new Thread(){
        //         @Override
        //         public void run() {
        //             while (true){
        //                 try {
        //                     Thread.sleep(1);
        //                 } catch (InterruptedException e) {
        //                     e.printStackTrace();
        //                 }
        //                 count2 += 1;
        //             }
        //         }
        //     };
        //     Thread th3 = new Thread(){
        //         @Override
        //         public void run() {
        //             try {
        //                 Thread.sleep(1000);
        //                 System.out.printf("%d, %d",count1, count2);
        //             } catch (InterruptedException e) {
        //                 e.printStackTrace();
        //             }
        //         }
        //     };
        //     th1.start();
        //     th2.start();
        //     th3.start();
        //     while(true){
        //         ;
        //     }
        // }

//     @Test
//     public void keyedFeatureTest() throws Exception {
//         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//         env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//         // start the data generator
//         DataStream<RawData> input = env
//                 .addSource(new DataSource())
//                 .setParallelism(1);

//         DataStream<Feature> features = Utils.computeInputSignal(input);
//         DataStream<DetectedEvent> result = Query1Streaming.start(features);
//         result.print();
//         env.execute("Number of busy machines every 5 minutes over the last 15 minutes");
//     }

//     @Test
//     public void predictTest() throws Exception{
//         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//         env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//         // start the data generator
//         DataStream<RawData> input = env
//                 .addSource(new DataSource(500))
//                 .setParallelism(1);

//         DataStream<Feature> features = Utils.computeInputSignal(input);
//         DataStream<DetectedEvent> result = Query1Streaming.start(features);
// //        result.print();
//         env.execute("Number of busy machines every 5 minutes over the last 15 minutes");
//     }

    @Test
    public void testPostMethod() throws Exception{

    }

}
