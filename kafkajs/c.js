const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'c2',
  brokers: ['127.0.0.1:9092'],
  //requestTimeout: 10000,
  //retry: {
  //  initialRetryTime: 100,
  //  initialRetryTime: 1000,
  //  retries: 8
  //}

});


//const producer = kafka.producer()

const consumer = kafka.consumer({ 
    groupId: 'g4', 
    //maxWaitTimeInMs:100, 

 })

const run = async () => {
  // Producing
  //await producer.connect()
  //await producer.send({
  //  topic: 't2',
  //  messages: [
  //    { value: 'a' +  (new Date().getTime()) },
  //  ],
  //})

  // Consuming
  await consumer.connect()
  await consumer.subscribe({ topic: 't2', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      /*
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      })
      */
      
      console.log('got msg4 ' + message.value.toString() + ' in ' + ((new Date().getTime()) - message.value.toString().replace('a', '')*1) );
      
    },
  })
}

run().catch(console.error)
