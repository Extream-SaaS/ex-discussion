const Firestore = require('@google-cloud/firestore');
const admin = require('firebase-admin');
const projectId = 'stoked-reality-284921';

const publish = (
  topicName = 'ex-gateway',
  source = 'app-engine',
  data = {}
) => {
  const {PubSub} = require('@google-cloud/pubsub');
  // Instantiates a client
  const pubsub = new PubSub({projectId});

  async function publishMessage() {
    const dataBuffer = Buffer.from(JSON.stringify(data));

    const messageId = await pubsub.topic(`${topicName}-${source}`).publish(dataBuffer);
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
  const {domain, action, command, socketId, payload, user, source} = message;
  const db = new Firestore({
    projectId,
  });
  if (message.payload.start_date) {
    message.payload.start_date = Firestore.Timestamp.fromDate(new Date(Date.parse(message.payload.start_date)));
  }
  if (message.payload.end_date) {
    message.payload.end_date = Firestore.Timestamp.fromDate(new Date(Date.parse(message.payload.end_date)));
  }
  switch (command) {
    case 'create':
      try {
        const docRef = db.collection('rooms').doc();
    
        await docRef.set({
          ...payload,
          addedBy: user.id,
          addedAt: Firestore.FieldValue.serverTimestamp()
        });
   
        await Promise.all([
          publish('ex-manage', { domain, action, command, payload: { ...payload, id: docRef.id }, user, socketId }),
          publish('ex-gateway', source, { domain, action, command, payload: { ...payload, id: docRef.id }, user, socketId })
        ]);
        callback();
      } catch (error) {
        await publish('ex-gateway', source, { error: error.message, domain, action, command, payload, user, socketId });
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
        }, {
          merge: true
        });
    
        await publish('ex-gateway', source, { domain, action, command, payload: { ...payload }, user, socketId });
        callback();
      } catch (error) {
        await publish('ex-gateway', source, { error: error.message, domain, action, command, payload, user, socketId });
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
    
        await publish('ex-gateway', source, { domain, action, command, payload: room.data(), user, socketId });
        callback();
      } catch (error) {
        await publish('ex-gateway', source, { error: error.message, domain, action, command, payload, user, socketId });
        callback(0);
      }
      break;
    case 'get':
      try {
        const docRef = db.collection('rooms').doc(payload.id);
        const room = await docRef.get();

        if (!room.exists) {
          throw new Error('item not found');
        }

        let data = room.data();

        if (domain === 'client') {
          // we need to get all the instances and their statuses if we are a participant we can get the messages too
          const instancesRef = docRef.collection('instances');
          const instances = await instancesRef.get();
          data.instances = {};
          const participant = data.configuration.operators.includes(user.id);
          instances.forEach(async instance => {
            data.instances[instance.id] = instance.data();

            if (participant) {
              const messageRef = instancesRef.doc(instance.id).collection('messages');
              const messages = await messageRef.get();
              data.instances[instance.id].messages = {};

              messages.forEach(message => {
                data.instances[instance.id].messages[message.id] = message.data();
              });
            }
          });
        } else {
          if (data.configuration.mode) {
            // we need an instance ID
            if (!payload.data.instance) {
              throw new Error('instance is required');
            }
            const instanceRef = docRef.collection('instances').doc(payload.data.instance);
            const instance = await instanceRef.get();
            data.instance = instance.data();
            const participant = data.instance.participants.includes(user.id);
            if (participant) {
              const messageRef = instanceRef.collection('messages');
              const messages = await messageRef.get();
              data.messages = {};

              messages.forEach(message => {
                data.messages[message.id] = message.data();
              });
            }
          } else {
            const messageRef = docRef.collection('messages');
            const messages = await messageRef.get();

            data.messages = {};

            messages.forEach(message => {
              data.messages[message.id] = message.data();
            });
          }
        }
    
        await publish('ex-gateway', source, { domain, action, command, payload: { id: payload.id, ...data }, user, socketId });
        callback();
      } catch (error) {
        await publish('ex-gateway', source, { error: error.message, domain, action, command, payload, user, socketId });
        callback(0);
      }
      break;
    case 'start':
      try {
        console.log('payload', payload);
        const docRef = db.collection('rooms').doc(payload.id);
        const room = await docRef.get();

        if (!room.exists) {
          throw new Error('item not found');
        }

        let data = room.data();

        payload.data.operators = data.configuration.operators;
    
        const instanceRef = docRef.collection('instances').doc(payload.data.instance);

        await instanceRef.set({
          participants: [user.id],
          audience: (user.user_type === 'audience') ? user : null,
          status: 'pending',
          addedBy: user.id,
          addedAt: Firestore.FieldValue.serverTimestamp(),
        });
    
        await publish('ex-gateway', source, { domain, action, command, payload: { ...payload }, user, socketId });
        callback();
      } catch (error) {
        await publish('ex-gateway', source, { error: error.message, domain, action, command, payload, user, socketId });
        callback(0);
      }
      break;
    case 'activate':
      // client activates and sets status to active
      try {
        if (domain !== 'client') {
          callback(0);
        }
        let data = {};
        if (payload.data.instance) {
          const docRef = db.collection('rooms').doc(payload.id);
          const instanceRef = docRef.collection('instances').doc(payload.data.instance);
          await instanceRef.set({
            status: 'active',
            participants: admin.firestore.FieldValue.arrayUnion(user.id)
          }, { merge: true });
          const instance = await instanceRef.get();
          data = instance.data();
          const messageRef = instanceRef.collection('messages');
          const messages = await messageRef.get();
          data.messages = {};

          messages.forEach(message => {
            data.messages[message.id] = message.data();
          });
        } else {
          throw new Error('instance is required');
        }
        await publish('ex-gateway', source, { domain, action, command, payload: { ...payload, ...data }, user, socketId });
        callback();
      } catch (err) {
        await publish('ex-gateway', source, { error: error.message, domain, action, command, payload, user, socketId });
        callback(0);
      }
      break;
    case 'send':
      try {
        console.log('payload', payload);
        const docRef = db.collection('rooms').doc(payload.id);
    
        if (payload.data.instance) {
          const instanceRef = docRef.collection('instances').doc(payload.data.instance);
          const messageRef = instanceRef.collection('messages').doc(payload.data.uuid);
          await messageRef.set(payload.data);
        } else {
          const messageRef = docRef.collection('messages').doc(payload.data.uuid);
          await messageRef.set(payload.data);
        }
    
        await publish('ex-gateway', source, { domain, action, command, payload: { ...payload }, user, socketId });
        callback();
      } catch (error) {
        await publish('ex-gateway', source, { error: error.message, domain, action, command, payload, user, socketId });
        callback(0);
      }
      break;
    case 'remove':
      try {
        console.log('payload', payload);
        const docRef = db.collection('rooms').doc(payload.id);
    
        const messageRef = docRef.collection('messages').doc(payload.data.uuid);

        await messageRef.set(
          {
            removed: true,
            updatedBy: user.id,
            updatedAt: Firestore.FieldValue.serverTimestamp()
          },
          { merge: true }
        );

        await publish('ex-gateway', source, { domain, action, command, payload: { ...payload }, user, socketId });
        callback();
      } catch (error) {
        await publish('ex-gateway', source, { error: error.message, domain, action, command, payload, user, socketId });
        callback(0);
      }
      break;
    case 'ban':
      if (domain !== 'client') {
        callback(0);
      }
      try {
        console.log('payload', payload);
        const docRef = db.collection('rooms').doc(payload.id);
    
        const messageRef = docRef.collection('messages').doc(payload.data.uuid);

        await messageRef.set(
          {
            removed: true,
            updatedBy: user.id,
            updatedAt: Firestore.FieldValue.serverTimestamp()
          },
          { merge: true }
        );

        await publish('ex-gateway', source, { domain, action, command, payload: { ...payload }, user, socketId });
        callback();
      } catch (error) {
        await publish('ex-gateway', source, { error: error.message, domain, action, command, payload, user, socketId });
        callback(0);
      }
  }
};