/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package thebloomfilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

/**
 *
 * @author root
 */
public class TheBloomFilter {

    public static final int N = 127;
    public static final int STARTNUM = 1000000000;
    public static final int ELEMENTSNUM = 100;
    public static final int MAXELEM = 450;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Properties settings = new Properties();
        // Set a few key parameters
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "TheBloomFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "zookeeper:2181");

        // Create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(settings);
        KStreamBuilder builder = new KStreamBuilder();

        Serde<String> stringSerde = Serdes.String();
        KStream<String, Long> myStream = builder.stream(stringSerde, Serdes.Long(), "BloomFilterTopic");
        List<Long> S = createUniversalSet();
        //Initialize bit vector with Universal set
        int[] bitVector = loadUniversalSet(S);
        //printVector(bitVector);
        S = null;
        myStream.foreach(new ForeachAction<String, Long>() {

            public int elementsFiltered = 0;
            public int count = 0;

            @Override
            public void apply(String k, Long v) {

                count++;
                System.out.println("hello");
                if (bloomFilter(bitVector, v)) {
                    elementsFiltered++;
                }

                if (count == MAXELEM) {
                    count = 0;
                    System.out.println("BloomFilter completed");
                    writeAnswerIntoTopic(elementsFiltered);
                }
                
                

            }

            private boolean bloomFilter(int[] bitVector, Long v) {

                int i = 0;
                boolean flag = false;
                int[] kIndex = applyHashFunctions(v);
                while (i < kIndex.length && flag == false) {
                    if (kIndex[i] != -1 && bitVector[kIndex[i]] == 0) {
                        flag = true;
                    }
                    i++;
                }

                return flag;
            }

            private void writeAnswerIntoTopic(int count) {
                Properties props = new Properties();
                props.put("bootstrap.servers", "localhost:9092");
                props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                Producer<String, String> producer;
                producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

                String topicResponse = "Total elements sent: " + MAXELEM + " total elements filtered: " + count;
                ProducerRecord<String, String> record = new ProducerRecord<>("BloomFilterResult", "key", topicResponse);
                producer.send(record);
                producer.close();
            }

        });

        KafkaStreams stream = new KafkaStreams(builder, config);
        System.out.println("Bloom FIlter Streaming started !");
        stream.start();
    }

    private static List<Long> createUniversalSet() {

        List<Long> l = new ArrayList<>();
        for (long i = STARTNUM; i < STARTNUM + ELEMENTSNUM; i++) {
            l.add(i);
        }
        return l;
    }

    private static int[] loadUniversalSet(List<Long> s) {
        int[] bitVector = initializeVector(N);
        int[] indexes;
        for (long num : s) {
            indexes = applyHashFunctions(num);
            for (int i : indexes) {
                if (i != -1) {
                    bitVector[i] = 1;
                }
            }
        }

        return bitVector;
    }

    private static int[] applyHashFunctions(long l) {

        String s = Long.toBinaryString(l);
        //Take the even bits and generate a number
        int x = generateIndex(s, true);
        //Take the odd bits and generate a number
        int y = generateIndex(s, false);

        int[] indexes = new int[]{-1, -1, -1, -1, -1};
        //Hash function 1
        indexes[0] = x % N;
        indexes[1] = y % N;

        return indexes;

    }

    private static int generateIndex(String s, boolean b) {

        int index = 1;
        StringBuilder sb = new StringBuilder();
        if (b) {
            index = 0;
        }

        while (index < s.length()) {
            sb.append(s.charAt(index));
            index += 2;
        }

        return Integer.parseInt(sb.toString(), 2);
    }

    private static int[] initializeVector(int n) {
        int[] v = new int[n];
        for (int i = 0; i < n; i++) {
            v[i] = 0;
        }

        return v;
    }
    
    private static void printVector(int[] bitVector) {
		
		for(int x : bitVector){
			System.out.print(" "+x+" ");
		}
		
    }

}
