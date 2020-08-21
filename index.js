const Firestore = require('@google-cloud/firestore');
const projectId = 'stoked-reality-284921';

const publish = (
  topicName = 'ex-gateway',
  data = {}
) => {
  const {PubSub} = require('@google-cloud/pubsub');
  // Instantiates a client
  const pubsub = new PubSub({projectId});

  async function publishMessage() {
    const dataBuffer = Buffer.from(JSON.stringify(data));

    const messageId = await pubsub.topic(topicName).publish(dataBuffer);
    return messageId;
  }

  return publishMessage();
};

/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
exports.manage = async (event, context, callback) => {
  const message = event && event.data ? JSON.parse(Buffer.from(event.data, 'base64').toString()) : null;
  if (message === null) {
    callback();
  }
  const {domain, action, command, socketId, payload, user} = message;
  const db = new Firestore({
    projectId,
  });

  try {
    const docRef = db.collection('rooms').doc(message.itinerary_id);

    await docRef.set({
      messages: [],
      title: message.title,
      start_date: message.start_date,
      end_date: message.end_date,
      itinerary: message.itinerary,
      landing_page: message.landing_page,
      type: message.type,
      configuration: message.configuration
    });
    console.log(docRef);

    publish('ex-gateway', { domain, action, command, payload: { ...payload, public_id: organisation.public_id }, user, socketId });
    callback();
  } catch (error) {
    publish('ex-gateway', { error: error.message, domain, action, command, payload, user, socketId });
    callback(0);
  }
};