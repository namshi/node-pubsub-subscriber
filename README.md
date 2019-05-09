# pubsub-subscriber

An opinionated GCP Pub/Sub subscriber we use here at [Namshi](https://github.com/namshi).

## Usage

``` js
let {subscribe} = require('pubsub-subscriber')

// name of the topic you want to listen to
let topic = "my_topic" 

// name of your subscription, will be created automagically
let subscription = "my_topic_send_email"

// function that processes the message:
// payload will be the JSON parsed.
// 
// Return to ack, throw an exception to nack.
async function onMessage(payload, options) {
    if (payload.ok) {
        return
    }
    
    // options.message - the original message object from the pubsub SDK
    // options.topic_name - the topic you've subscribed to

    throw new Error("I f'ed up")
}

subscribe(topic, subscription, onMessage)
```

That's it!
