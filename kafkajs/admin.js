
//usage: 
//node admin.js 
//or 
//node amdin.js 1 ...
var arg3 = process.argv.length>=3 ? process.argv[2] : '';
var args = process.argv;

console.log('admin:');
//console.log('admin', process.argv);
//console.log(arg3);


const { Kafka } = require('kafkajs');
const { ConfigResourceTypes } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'c2',
  brokers: ['127.0.0.1:9092'],
});

const admin = kafka.admin();





const run = async () => {
    await admin.connect();

    // list topic
    // node amdin.js topic_list
    if (['topics', 'topic_list'].includes(arg3)){
        $topics = await admin.listTopics();
        console.log('topic_list')
        console.log($topics);
    }



    // create topic
    //node admin.js topic_create t4 4
    if ([ 'topic_create'].includes(arg3)){
        let topic = args[3];
        let pars = args[4]*1;
        let res = await admin.createTopics({
            validateOnly: false,
            topics: [
                {
                    topic: args[3], 
                    numPartitions: pars 
                }
            ]    
        })
        console.log('topic_create', topic, pars, res);
    }


    //creat topic partition
    //node admin.js partition_create t4 6
    if ([ 'partition_create'].includes(arg3)){
        let topic = args[3];
        let pars = args[4]*1;
        let res = await admin.createPartitions({
            validateOnly: false,
            topicPartitions:[
                {
                    topic: topic,
                    count: pars
                }
            ]    
        })
        console.log('partition_create', topic, pars, res);
    }


    // topic meta data
    //node admin.js topic_meta t4
    if ([ 'topic_meta'].includes(arg3)){
        let topic = args[3];

        let res = await admin.fetchTopicMetadata({
            topics: [topic], 
            timeout: 3000  
        })
        console.log('topic_meta', 'partitions')
        console.log(res.topics[0].partitions);
        //console.log(res);
    }


    // get topic offsets
    //  node admin.js topic_offsets t4
    if ([ 'topic_offsets'].includes(arg3)){
        let topic = args[3];
        let res = await admin.fetchTopicOffsets(topic)
        console.log('topic_offsets')
        console.log(res);
    }




    if (['topic_conf'].includes(arg3)){
        let topic = args[3];
        let k = await admin.describeConfigs({
          includeSynonyms: false,
          resources: [
            {
              type: ConfigResourceTypes.TOPIC,
              name: topic,
              //configNames: ['cleanup.policy']
            }
          ]
        })
        console.log('k', k.resources[0].configEntries);
    }



    //list groups
    //node admin.js groups
    if (['groups', 'group_list'].includes(arg3)){

        let res = await admin.listGroups();
        console.log('group_list');
        console.log(res.groups);
    }


    // describe group
    // node amdin.js group_des g7
    if (['group_des'].includes(arg3)){
        let group = arg3;
        let res = await admin.describeGroups([arg3]);
        console.log('group_des');
        console.log(res.groups);
    }

    if (['group_offsets'].includes(arg3)){
        let topic = args[3];
        let groupId = args[4]
        let res = await admin.fetchOffsets({ groupId, topic})
        console.log('group offsets', topic, groupId);
        console.log(res);
    }


    // describe cluster
    // node amdin.js cluster_des
    if (['cluster_des'].includes(arg3)){
        let group = arg3;
        let res = await admin.describeCluster();
        console.log('cluster_des');
        console.log(res);
    }



    





    /*
    // delte topic
    // dont use,  will break kafka
    if ([ 'topic_del'].includes(arg3)){
        let topic = args[3];

        let res = await admin.deleteTopics({
            topics: [topic], 
            timeout: 3000  
        })
        console.log('topic_del', topic,  res);
    }
    */




    await admin.disconnect();
}

run().catch(console.error)






