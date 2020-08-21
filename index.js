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
  if (message.payload.start_date) {
    console.log('start date parsed', Date.parse(message.payload.start_date));
    message.payload.start_date = Firestore.Timestamp.fromDate(Date.parse(message.payload.start_date));
  }
  if (message.payload.end_date) {
    console.log('end date parsed', Date.parse(message.payload.end_date));
    message.payload.end_date = Firestore.Timestamp.fromDate(Date.parse(message.payload.end_date));
  }
  switch (command) {
    case 'create':
      try {
        const docRef = db.collection('rooms').doc();
    
        await docRef.set({
          messages: [],
          ...payload,
          addedBy: user.id,
          addedAt: Firestore.FieldValue.serverTimestamp()
        });
   
        await publish('ex-gateway', { domain, action, command, payload: { ...payload, id: docRef.id }, user, socketId });
        callback();
      } catch (error) {
        await publish('ex-gateway', { error: error.message, domain, action, command, payload, user, socketId });
        callback(0);
      }
      break;
    case 'update':
      try {
        const docRef = db.collection('rooms').doc(payload.id);
    
        await docRef.set({
          ...payload,
          updatedBy: user.id,
          updatedAt: Firestore.FieldValue.serverTimestamp()
        });
    
        await publish('ex-gateway', { domain, action, command, payload: { ...payload }, user, socketId });
        callback();
      } catch (error) {
        await publish('ex-gateway', { error: error.message, domain, action, command, payload, user, socketId });
        callback(0);
      }
      break;
    case 'read':
      try {
        const docRef = db.collection('rooms').doc(payload.id);
    
        const room = await docRef.get();

        if (!room.exists) {
          throw new Error('item not found');
        }
    
        await publish('ex-gateway', { domain, action, command, payload: room.data(), user, socketId });
        callback();
      } catch (error) {
        await publish('ex-gateway', { error: error.message, domain, action, command, payload, user, socketId });
        callback(0);
      }
      break;
  }
};