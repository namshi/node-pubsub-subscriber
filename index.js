const { PubSub } = require("@google-cloud/pubsub");
const logger = require("lib-logger");

async function subscribe(topic_name, subscription_name, subscriber, opts = {}) {
  const pubsub = new PubSub();
  try {
    logger.info(`Creating subscription ${subscription_name} on topic ${topic_name}`)
    await pubsub.topic(topic_name).createSubscription(subscription_name);
    logger.info(`Subscription ${subscription_name} on topic ${topic_name} created successfully`)
  } catch(err) {
    if (err.code === 6) {
      logger.info(`subscription ${subscription_name} already exist, skipping creating it...`)
    } else {
        throw err;
    }
  }

  const subscription = pubsub.subscription(subscription_name, {
    flowControl: {
      maxMessages: 10,
    },
  });

  logger.info(`Listening...`);
  subscription.on(`message`, async function processMessage(message) {
    const { data, publishTime, received } = message;
    const { delay = 0 } = opts;

    if (delay && (received / 1000 - publishTime.getFullTimeString() / 1000000000) < delay) {
      message.nack(delay);
    }
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