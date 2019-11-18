const { PubSub } = require("@google-cloud/pubsub");
const logger = require("lib-logger");

async function subscribe(topic_name, subscription_name, subscriber, options) {
  const pubsub = new PubSub();
  // Default subscriber options
  // Documentation here https://cloud.google.com/pubsub/docs/pull#config
  const defaultOptions = {
    flowControl: {
      maxMessages: 10,
    },
  };

  const topic = pubsub.topic(topic_name);
  const subscription = topic.subscription(subscription_name, options || defaultOptions);

  try {
    const [exists] = await subscription.exists();
    if (!exists) {
      logger.info(`Creating subscription ${subscription_name} on topic ${topic_name}`);
      await subscription.create();
      logger.info(`Subscription ${subscription_name} on topic ${topic_name} created successfully`);
    }
  } catch(err) {
    logger.error(`Error checking subscription exists or creating subscription`, err);
    message.nack();
    return;
  }

  logger.info(`Listening...`);
  subscription.on(`message`, async function processMessage(message) {
    const { data } = message;
    try {
      let payload = {};
      try {
        payload = JSON.parse(data);
      } catch (e) {
        logger.error(`Invalid message received: ${data.toString("utf8")}`);
        message.ack();
        return;
      }
      await subscriber(payload, { message, topic_name });
      message.ack();
      logger.info(payload);
    } catch (e) {
      logger.error(`error processing: ${data.toString("utf8")} - ${e.message}`);
      message.nack();
    }
  });
}

module.exports = {
  subscribe
}

process.on('unhandledRejection', err => { throw err })