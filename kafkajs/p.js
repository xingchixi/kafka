const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'c2',
  brokers: ['127.0.0.1:9092'],
});


const producer = kafka.producer()


const run = async () => {
  // Producing
  await producer.connect()
  await producer.send({
    topic: 't2',
    messages: [
      { value: 'a' +  (new Date().getTime()) },
    ],
  })

process.exit(1)

}

run().catch(console.error)
