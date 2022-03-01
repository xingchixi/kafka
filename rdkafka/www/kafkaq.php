<?php


//kafka queue
class QueueKafkaEx {

    private $topic;
    private $group;

    private $producer;
    private $producerTopic; 

    private $consumer;

    private $t;

    public function __construct($queueName, $group) {
        $this->topic = $queueName;
        $this->group = $group;

        $this->t = microtime(true);

    }

    public  function iniProducer() {
        if ($this->producer!== null) {
            return;
        }    

        $conf = new RdKafka\Conf();
        $conf->set('metadata.broker.list', '127.0.0.1:9092');
        $this->producer = new RdKafka\Producer($conf);
        $this->producerTopic = $this->producer->newTopic($this->topic);
        
        
    }

    public function iniConsumer() {
        if ($this->consumer !== null) {
            return;
        }

        echo "real ini consumer \n";

        $conf = new RdKafka\Conf();

        // Set a rebalance callback to log partition assignments (optional)
        $conf->setRebalanceCb(function (RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    echo "Assign partitions. " .(microtime(true) - $this->t). "\n";
                    //var_dump($partitions);
                    $kafka->assign($partitions);
                    break;

                 case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                     echo "Revoke partitions.\n";
                     //var_dump($partitions);
                     $kafka->assign(NULL);
                     break;

                 default:
                    throw new \Exception($err);
            }
        });


        $conf->set('group.id', $this->group);
        $conf->set('metadata.broker.list', '127.0.0.1');

        // Set where to start consuming messages when there is no initial offset in
        // offset store or the desired offset is out of range.
        // 'earliest': start from the beginning
        $conf->set('auto.offset.reset', 'earliest');

        $this->consumer = new RdKafka\KafkaConsumer($conf);
        $this->consumer->subscribe([$this->topic]);

    }

    public function enqueue($data) {
        echo "enqueue \n";
        $this->iniProducer();
        
        $msg = json_encode($data);   
        $this->producerTopic->produce(RD_KAFKA_PARTITION_UA, 0, $msg);
        $this->producer->poll(0);
        

        for ($flushRetries = 0; $flushRetries < 10; $flushRetries++) {
            $result = $this->producer->flush(10000);
            if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) {
                break;
            }
        }

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
            throw new \RuntimeException('Was unable to flush, messages might be lost!');
        }

    }


    // first dequeue need to assign partitions, take longer. on local is like 3 sec for assgning partition
    // if there is data, will return instantly (after assign partition);
    // if no data, will wait till time out. then return null
    public function dequeue() {
        echo "dequeue " .(microtime(true) - $this->t). "----------------\n";
        
        $this->iniConsumer();

        $timeout = 10 * 1000;               // in ms
        $message = $this->consumer->consume($timeout);
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                //echo "get message in " .(microtime(true) - $this->t). "\n";
                //echo $message->payload;
                //var_dump($message);
                //echo "\n\n";

                $data = json_decode($message->payload);
                return $data;

                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                echo "No more messages; will wait for more\n";
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                echo "Timed out\n";
                break;
            default:
                throw new \Exception($message->errstr(), $message->err);
                break;
        }

    }

}


$q1 = new QueueKafkaEx('q1', 'g1');
for($i = 0; $i<1; $i++){
    //$q1->enqueue("data" . $i);
}

$q1->iniConsumer();
echo "sleeping \n";
//sleep(3);
echo "sleeped \n";

for($j=0; $j<1; $j++){
   var_dump(['x', $q1->dequeue()]); 
}


