const amqp = require('amqplib');
const WORK_QUEUE = require('./constants')['WORK_QUEUE']
const JOB_TYPES = require('./constants')['JOB_TYPES']

const AWS = require('aws-sdk')

let glacier = null
let current_region = null

function getClient(region){
  if(!current_region || current_region != region) {
    console.log(`Selected region: ${region}`)
    glacier = new AWS.Glacier({apiVersion: '2012-06-01', region: region})
    current_region = region
  }
  return glacier
}

function handleJob(job){
  let glacier = getClient(job.region)

  switch (job.type) {
    case JOB_TYPES.LIST_VAULTS:
      req = glacier.listVaults({accountId: '-'})

      return new Promise((resolve, reject) => {
        req.on('success', (res) => {
          resolve(res.data['VaultList'])
        }).on('error', (res) => {
          reject(res.error)
        })
        req.send()
      })
      break;
    default:
      console.log(`Job ${job.type} not supported`)
      return null
      break;
  }
}

amqp.connect('amqp://localhost').then(function(conn){
  
  console.log("Connected to RabbitMQ")

  conn.createChannel().then(function(ch){
    console.log("Channel established")
    ch.assertQueue(WORK_QUEUE)
    ch.prefetch(1)

    console.log(`Waiting for job from queue ${WORK_QUEUE}`)
    
    ch.consume(WORK_QUEUE, function(msg){

      let job = JSON.parse(msg.content.toString())

      handleJob(job).then((data) => {
        ch.sendToQueue(msg.properties.replyTo, Buffer.from(JSON.stringify(data)), {correlationId: msg.properties.correlationId})
      },(err) => {
        ch.sendToQueue(msg.properties.replyTo, Buffer.from("ERROR!!!"), {correlationId: msg.properties.correlationId})
      })
      
    }, {noAck: true})
  },function(err){
      console.log("Cannot establish RabbitMQ channel")
  })

}, function(err){
  console.log("Cannot connect to RabbitMQ")
})