<?php
namespace app\utils;

/**
 * rabbit mq lib
 * Class LibRabbit
 */

class LibRabbit{

    private $conn;
    private $channel;
    private $exchange;
    private $queue;

    public function __construct(){
        try {
            $this->conn = new \AMQPConnection(RABBIT_CONF);
            if (!$this->conn->connect()) {
                throw new \Exception('Cannot connect to the broker');
            }
        } catch (\Exception $e) {
            throw new \Exception('cannot connection rabbitMq:' . $e->getMessage());
        }
    }

    private function setChannel(){
        $this->channel = new \AMQPChannel($this->conn);
    }

    /**
     * 设置交换机
     * @throws \AMQPConnectionException
     * @throws \AMQPExchangeException
     */
    private function setExchange(){
        $this->exchange = new \AMQPExchange($this->channel);
        $this->exchange->setName(RABBIT_CONF['exchange']);
        $this->exchange->setType(RABBIT_CONF['exchangeType']); //direct类型
        $this->exchange->setFlags(\AMQP_DURABLE); //持久化
    }

    /**
     * 设置队列名称
     * @throws \AMQPConnectionException
     * @throws \AMQPQueueException
     */
    private function setQueuename(){
        //创建队列
        $this->queue = new \AMQPQueue($this->channel);
        $this->queue->setName(RABBIT_CONF['queueName']);
        $this->queue->setFlags(\AMQP_DURABLE); //持久化
    }
    private function bind(){
        //绑定交换机与队列，并指定路由键
        $this->queue->bind(RABBIT_CONF['exchange'] , RABBIT_CONF['route']);
    }

    /**
     * 写入队列
     * @param $msgBody
     * @return string
     * @throws \AMQPConnectionException
     * @throws \AMQPExchangeException
     */
    public function push($msgBody){
        if (is_array($msgBody)) {
            $msgBody = json_encode($msgBody);
        }
        $id = uniqid();
        $this->setChannel();
        $this->setExchange();
        $this->setQueuename();
        $this->exchange->publish($msgBody , RABBIT_CONF['route']);
        return $id;
    }

    /**
     * 消费队列
     * @param $callback
     */
    public function consume($callback){
        $this->setChannel();
        $this->setExchange();
        $this->setQueuename();
        $this->bind();
        $this->queue->consume($callback);
    }

    public function __destruct()
    {
        $this->conn->disconnect();
    }
}
