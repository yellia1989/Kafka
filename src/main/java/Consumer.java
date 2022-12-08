import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers",
                "bigdata01:9092,bigdata02:9092,bigdata03:9092");
        props.put("group.id", "test-group");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("heartbeat.interval.ms", 1000);

        // 如果说kafka broker在10秒内感知不到一个consumer心跳就会认为那个consumer挂了，此时会触发rebalance
        props.put("session.timeout.ms", 10 * 1000);

        // 如果30秒才去执行下一次poll
        // 如果说某个consumer挂了，kafka broker感知到了，会触发一个rebalance的操作，就分配他的分区
        // 给其他的cosumer来消费，其他的consumer如果要感知到rebalance重新分配分区，就需要通过心跳来感知
        // 心跳的间隔一般不要太长，1000，500
        props.put("max.poll.interval.ms", 30 * 1000);

        props.put("fetch.max.bytes", 10485760);
        // 如果说你的消费的吞吐量特别大，此时可以适当提高一些
        props.put("max.poll.records", 500);
        // 不要去回收那个socket连接
        props.put("connection.max.idle.ms", -1);
        // 开启自动提交，他只会每隔一段时间去提交一次offset
        // 如果你每次要重启一下consumer的话，他一定会把一些数据重新消费一遍
        props.put("enable.auto.commit", "true");
        // 每次自动提交offset的一个时间间隔
        props.put("auto.commit.ineterval.ms", "1000");
        // 每次重启都是从最早的offset开始读取，不是接着上一次
        props.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList("topic1"));
        try {
            while(true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Integer.MAX_VALUE);
                for(ConsumerRecord<String, String> record : records) {
                    System.out.println("key: " + record.key() + ", value: " + record.value());
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
            consumer.close();
        }
    }
}
