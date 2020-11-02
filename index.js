const Firestore = require('@google-cloud/firestore');
const admin = require('firebase-admin');
const projectId = 'stoked-reality-284921';

const publish = (
  topicName,
  source,
  data
) => {
  const {PubSub} = require('@google-cloud/pubsub');
  // Instantiates a client
  const pubsub = new PubSub({projectId});

  async function publishMessage() {
    const sourceStr = data ? `-${source}` : '';
    const dataBuffer = Buffer.from(JSON.stringify(!data ? source : data));
    console.log('pushing to', `${topicName}${sourceStr}`);
    const messageId = await pubsub.topic(`${topicName}${sourceStr}`).publish(dataBuffer);
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
    
        await publish('ex-gateway', source, { domain, action, command, payload: { ...payload, ...room.data() }, user, socketId });
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
            // we need an instance ID - if not provided, lets get all the users instances
            if (!payload.data) {
              const instancesRef = docRef.collection('instances');
              const myInstances = await docRef.collection('instances').where('from.id', '==', user.id).get();
              const publicUser = (({email, token, ...user}) => user)(user);
              console.log(publicUser);
              const joinedInstances = await docRef.collection('instances').where('participants', 'array-contains', publicUser).get();
              const instances = joinedInstances.docs.concat(myInstances.docs);
              data.instances = {};

              instances.forEach(async instance => {
                data.instances[instance.id] = instance.data();
                const messageRef = instancesRef.doc(instance.id).collection('messages');
                const messages = await messageRef.get();
                data.instances[instance.id].messages = {};
  
                messages.forEach(message => {
                  data.instances[instance.id].messages[message.id] = message.data();
                });
              });
            } else if (!payload.data.instance) {
              throw new Error('instance is required');
            } else {
              const instanceRef = docRef.collection('instances').doc(payload.data.instance);
              const instance = await instanceRef.get();
              if (!instance.exists) {
                throw new Error('instance not found');
              }
              data.instance = instance.data();
              console.log('instance retrieved', payload.data.instance, data);
              const participant = data.instance.participants.includes(user.id);
              if (participant) {
                const messageRef = instanceRef.collection('messages');
                const messages = await messageRef.get();
                data.messages = {};

                messages.forEach(message => {
                  data.messages[message.id] = message.data();
                });
              }
            }
          } else if (data.configuration.moderation && data.configuration.moderation === 'pre-moderate') {
            const messageRef = docRef.collection('messages');
            // if i am a moderator, show me all messages, else only show me mine
            let messages;
            if (data.configuration.moderators.includes(user.id)) {
              messages = await messageRef.get();
            } else {
              // show me all my messages, and public ones
              const myMessages = await messageRef.where('from.id', '==', user.id).get();
              const replyMessages = await messageRef.where('requester.id', '==', user.id).get();
              const publicMessages = await messageRef.where('private', '==', false).get();
              messages = myMessages.docs.concat(replyMessages.docs, publicMessages.docs);
            }

            data.messages = {};

            messages.forEach(message => {
              data.messages[message.id] = message.data();
            });
          } else {
            const messageRef = docRef.collection('messages');
            const messages = await messageRef.get();

            data.messages = {};

            messages.forEach(message => {
              data.messages[message.id] = message.data();
            });
          }
        }
    
        await publish('ex-gateway', source, { domain, action, command, payload: { ...payload, ...data }, user, socketId });
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
        
        const instanceRef = docRef.collection('instances').doc(payload.data.instance);
        let participants = [];

        if (data.configuration.mode === 'round-robin') {
          payload.data.operators = data.configuration.operators;
          participants.push(user.id);
        } else if (data.configuration.mode === 'direct') {
          participants = payload.data.participants;
        }
        await instanceRef.set({
          ...payload.data,
          participants,
          audience: (data.configuration.mode === 'round-robin') ? user : null,
          status: data.configuration.mode === 'round-robin' ? 'pending': 'active',
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
          const room = await docRef.get();

          if (!room.exists) {
            throw new Error('item not found');
          }

          let data = room.data();

          payload.data.operators = data.configuration.operators;
          const instanceRef = docRef.collection('instances').doc(payload.data.instance);
          await instanceRef.set({
            status: 'active',
            participants: admin.firestore.FieldValue.arrayUnion(user.id)
          }, { merge: true });
          const instance = await instanceRef.get();
          payload.data.instance = instance.data();
          payload.data.id = instance.id;
          const messageRef = instanceRef.collection('messages');
          const messages = await messageRef.get();
          payload.data.messages = {};

          messages.forEach(message => {
            payload.data.messages[message.id] = message.data();
          });
        } else {
          throw new Error('instance is required');
        }
        await publish('ex-gateway', source, { domain, action, command, payload: { ...payload }, user, socketId });
        callback();
      } catch (error) {
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
          if (payload.data.private === false && payload.data.parent) {
            // need to make the parent public too otherwise thread won't exist
            const parentRef = docRef.collection('messages').doc(payload.data.parent);
            await parentRef.set({
              private: false,
            }, { merge: true });
          }
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
    
        if (payload.data.instance) {
          const instanceRef = docRef.collection('instances').doc(payload.data.instance);
          const messageRef = instanceRef.collection('messages').doc(payload.data.uuid);
          await messageRef.set(
            {
              removed: true,
              updatedBy: user.id,
              updatedAt: Firestore.FieldValue.serverTimestamp()
            },
            { merge: true }
          );
        } else {
          const messageRef = docRef.collection('messages').doc(payload.data.uuid);
          await messageRef.set(
            {
              removed: true,
              updatedBy: user.id,
              updatedAt: Firestore.FieldValue.serverTimestamp()
            },
            { merge: true }
          );
        }

        await publish('ex-gateway', source, { domain, action, command, payload: { ...payload }, user, socketId });
        callback();
      } catch (error) {
        await publish('ex-gateway', source, { error: error.message, domain, action, command, payload, user, socketId });
        callback(0);
      }
      break;
    case 'add':
      try {
        console.log('payload', payload);
        const docRef = db.collection('rooms').doc(payload.id);
        const room = await docRef.get();

        if (!room.exists) {
          throw new Error('item not found');
        }

        let data = room.data();
        
        const instanceRef = docRef.collection('instances').doc(payload.data.instance);
        const instance = await instanceRef.get();
        if (!instance.exists) {
          throw new Error('instance not found');
        }
        const instanceData = instance.data();
        let participants = instanceData.participants;

        if (data.configuration.mode === 'direct') {
          participants = participants.concat(payload.data.participants);
        }
        await instanceRef.set({
          participants,
          updatedBy: user.id,
          updatedAt: Firestore.FieldValue.serverTimestamp()
        },
        { merge: true });

        payload.data.topic = instanceData.topic;
        payload.data.from = user;
        payload.data.route = instanceData.route;
    
        await publish('ex-gateway', source, { domain, action, command, payload: { ...payload }, user, socketId });
        callback();
      } catch (error) {
        await publish('ex-gateway', source, { error: error.message, domain, action, command, payload, user, socketId });
        callback(0);
      }
      break;
    case 'leave':
      try {
        console.log('payload', payload);
        const docRef = db.collection('rooms').doc(payload.id);
        const room = await docRef.get();

        if (!room.exists) {
          throw new Error('item not found');
        }

        let data = room.data();
        
        const instanceRef = docRef.collection('instances').doc(payload.data.instance);
        const instance = await instanceRef.get();
        if (!instance.exists) {
          throw new Error('instance not found');
        }
        let participants = instance.data().participants;

        if (data.configuration.mode === 'direct') {
          participants = participants.filter(participant => participant !== user.id);
        }
        await instanceRef.set({
          participants,
          updatedBy: user.id,
          updatedAt: Firestore.FieldValue.serverTimestamp()
        },
        { merge: true });
    
        await publish('ex-gateway', source, { domain, action, command, payload: { ...payload }, user, socketId });
        callback();
      } catch (error) {
        await publish('ex-gateway', source, { error: error.message, domain, action, command, payload, user, socketId });
        callback(0);
      }
      break;
  }
};