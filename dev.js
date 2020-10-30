require('dotenv').config();
const { manage } = require('./index');
let payload, data, event;
// associate itinerary item
// payload = {
//   domain: 'admin',
//   action: 'item',
//   command: 'create',
//   payload: {
//     itinerary: '0d84ac76-5d00-4969-9312-440900fa6eb3',
//     id: 'VrtZIutGiSghmnmeqXyl'
//   }
// };
// data = Buffer.from(JSON.stringify(payload)).toString('base64');
// event = {
//   data
// };
// manage(event, '', (resp) => {
//   console.log(resp);
//   console.log('executed');
// });

// // associate itinerary item
// payload = {
//   domain: 'admin',
//   action: 'itinerary',
//   command: 'read',
//   payload: {
//     id: '0d84ac76-5d00-4969-9312-440900fa6eb3'
//   },
//   user: {
//     id: "091e8b52-8506-4512-b75e-149ee51c4f04",
//     username: "tester",
//     fields: {
//       custom: "fields"
//     }
//   }
// };
// data = Buffer.from(JSON.stringify(payload)).toString('base64');
// event = {
//   data
// };
// manage(event, '', (resp) => {
//   console.log(resp);
//   console.log('executed');
// });

// associate itinerary item
// payload = {
//   domain: 'admin',
//   action: 'event',
//   command: 'create',
//   payload: {
//     name: "event-numero-2",
//     website: "www.example.com",
//     start_date: "2020-08-21 08:00:00",
//     end_date: "2020-08-22 17:00:00",
//     organisation: "d8ad68bd-9fcc-425e-b6ce-46c1c6ea7473"
//   },
//   user: {
//     id: "091e8b52-8506-4512-b75e-149ee51c4f04",
//     username: "tester",
//     fields: {
//       custom: "fields"
//     }
//   }
// };
// data = Buffer.from(JSON.stringify(payload)).toString('base64');
// event = {
//   data
// };

// manage(event, '', (resp) => {
//   console.log(resp);
//   console.log('executed');
// });

// // associate itinerary item
// payload = {
//   domain: 'admin',
//   action: 'event',
//   command: 'read',
//   payload: {
//     id: '3b739350-7626-4cf8-8bc7-7fefcc10fce8'
//   },
//   user: {
//     id: "091e8b52-8506-4512-b75e-149ee51c4f04",
//     username: "tester",
//     fields: {
//       custom: "fields"
//     }
//   }
// };
// data = Buffer.from(JSON.stringify(payload)).toString('base64');
// event = {
//   data
// };
// manage(event, '', (resp) => {
//   console.log(resp);
//   console.log('executed');
// });

// get events by id
// payload = {
//   domain: 'client',
//   action: 'notice',
//   command: 'send',
//   payload: { public_id: 'c1d218f0-7b8c-483f-9006-b4b5bfc6a0a7', event: '0bd12619-7f63-414c-8907-30b59aeb9380', message: { text: 'new message' } },
//   user: {
//     id: '8c3b38a3-e394-42dc-9c7e-5a741f238061',
//     username: 'richard@zest4.tv',
//     email: 'richard@zest4.tv',
//     fields: {
//       firstName: 'Rich',
//       lastName: 'Wilson',
//       company: 'Zest',
//       region: 'UK',
//       displayName: 'Rich - Crew'
//     },
//     user_type: 'chief',
//     eventId: '0bd12619-7f63-414c-8907-30b59aeb9380',
//     token: '57eb3bf9c362645229c6099a22292fb851cc0863'
//   },
//   socketId: 'h-LgfwquwW1XUDsuAAAB'
// };
// payload = {
//   domain: 'client',
//   action: 'notice',
//   command: 'get',
//   payload: { event: '0bd12619-7f63-414c-8907-30b59aeb9380'},
//   user: {
//     id: '8c3b38a3-e394-42dc-9c7e-5a741f238061',
//     username: 'richard@zest4.tv',
//     email: 'richard@zest4.tv',
//     fields: {
//       firstName: 'Rich',
//       lastName: 'Wilson',
//       company: 'Zest',
//       region: 'UK',
//       displayName: 'Rich - Crew'
//     },
//     user_type: 'chief',
//     eventId: '0bd12619-7f63-414c-8907-30b59aeb9380',
//     token: '57eb3bf9c362645229c6099a22292fb851cc0863'
//   },
//   socketId: 'h-LgfwquwW1XUDsuAAAB'
// };
// payload = {
//   domain: 'consumer',
//   action: 'notice',
//   command: 'read',
//   payload: { message: '996878a9-b6ca-44ef-9dba-36ec372c94dd'},
//   user: {
//     id: '8c3b38a3-e394-42dc-9c7e-5a741f238061',
//     username: 'tester',
//     fields: { custom: 'fields' },
//     token: 'e7c070e8d69b28093154bb7c4ca7602af8bd1cd4'
//   },
//   socketId: 'XbTiLsd9CmFwzEafAAAA'
// };
payload = {
  domain: 'consumer',
  action: 'chat',
  command: 'get',
  payload: {
    id: "A0IjU1Z2vznGTa172gfa",
    data: {},
  },
  user: {
    id: '71b0db19-6daf-4ec2-876b-fe068c64aed0',
    username: 'tester',
    fields: { custom: 'fields' },
    token: 'e7c070e8d69b28093154bb7c4ca7602af8bd1cd4'
  },
  socketId: 'XbTiLsd9CmFwzEafAAAA',
  source: 'rw-local',
};
data = Buffer.from(JSON.stringify(payload)).toString('base64');
event = {
  data
};
manage(event, '', (resp) => {
  console.log(resp);
  console.log('executed');
  process.exit();
});