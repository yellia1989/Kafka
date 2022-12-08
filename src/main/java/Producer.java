import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        // 这里可以配置几台broker即可，他会自动从broker去拉取元数据进行缓存
        props.put("bootstrap.servers",
                "bigdata01:9092,bigdata02:9092,bigdata03:9092");
        // 这个就是负责把发送的key从字符串序列化为字节数组
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        // 这个就是负责把你发送的实际的message从字符串序列化为字节数组
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("acks", "-1");
        props.put("retries", 3);
        props.put("batch.size", 323840);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("max.block.ms", 3000);

        // 创建一个Producer实例：线程资源，跟各个broker建立socket连接资源
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        ProducerRecord<String, String> record = new ProducerRecord<>("topic1", "test-key", "test-value");

        // 这是异步发送的模式
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if(exception == null) {
                    // 消息发送成功
                    System.out.println("消息发送成功");
                } else {
                    // 消息发送失败，需要重新发送
                    /*不管是异步还是同步，都可能让你处理异常，常见的异常如下：
                    1）LeaderNotAvailableException：这个就是如果某台机器挂了，此时leader副本不可用，会导致你
                    写入失败，要等待其他follower副本切换为leader副本之后，才能继续写入，此时可以重试发送即可。如果
                    说你平时重启kafka的broker进程，肯定会导致leader切换，一定会导致你写入报错，是
                            LeaderNotAvailableException
                    2）NotControllerException：这个也是同理，如果说Controller所在Broker挂了，那么此时会有问
                    题，需要等待Controller重新选举，此时也是一样就是重试即可
                    3）NetworkException：网络异常，重试即可
                    我们之前配置了一个参数，retries，他会自动重试的，但是如果重试几次之后还是不行，就会提供
                    Exception给我们来处理了。*/
                }
            }
        });
        Thread.sleep(10 * 1000);

        // 这是同步发送的模式
        //producer.send(record).get();
        // 你要一直等待人家后续一系列的步骤都做完，发送消息之后
        // 有了消息的回应返回给你，你这个方法才会退出来
        producer.close();
    }
}
