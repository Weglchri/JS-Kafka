var kafka = require('kafka-node');

var Consumer = kafka.Consumer;
var client = new kafka.KafkaClient();
var offset = new kafka.Offset(client);

//topic configuration
const topic = "dlc1";
const partition = 0;
const defaultoff = 1000 * 1000 * 1000;

//consumer configuration
var payloads = [{ topic: topic, partition: 0, offset: defaultoff }];
var options = { autoCommit: false, fromOffset: true, encoding: 'utf8', keyEncoding: 'utf8'};

//create consumer
var consumer = consumer = new Consumer(client, payloads, options);

//topic offset
offset.fetchLatestOffsets([topic], function (error, offsets) {
    if (error)
        console.log(error);
    consumer.setOffset(topic, partition, offsets[topic][0]-1)
});

consumer.on('message', function (message) {
    console.log("Consumer1 received: ", message.value);
});

consumer.on('error', function (err) {
    console.log('Error:',err);
})


