const amqp = require('amqplib');
const WORK_QUEUE = require('./constants')['WORK_QUEUE']
const JOB_TYPES = require('./constants')['JOB_TYPES']
const AWS = require('aws-sdk')
const setUpChannelWithRetry = require('./rabbitmq_connection')
const connectionRetryInterval = 1

let glacier = null
let current_region = null
let CHANNEL = null

function getClient(region){
  if(!current_region || current_region != region) {
    glacier = new AWS.Glacier({apiVersion: '2012-06-01', region: region})
    current_region = region
  }
  console.log(`Selected region: ${current_region}`)
  return glacier
}

function handleJob(job){
  switch (job.type) {
    case JOB_TYPES.LIST_VAULTS:
      return doListVaults(job);
      break;
    case JOB_TYPES.VAULT_INVENTORY:
      return doVaultInventory(job);
      break;
    case JOB_TYPES.LIST_JOBS:
      return doListJobs(job);
      break;
    default:
      console.log(`Job ${job.type} not supported`)
      return null
      break;
  }
}

function doListVaults(job) {
  console.log('doListVaults')
  req = getClient(job.region).listVaults({accountId: '-'})

  return promiseFromAWSRequest(req, 'VaultList')
}

function doVaultInventory(job) {
  let glacier = getClient(job.region)
  req = glacier.initiateJob({
    accountId: '-',
    vaultName: job.vaultName,
    jobParameters: {
      Type: 'inventory-retrieval'
    }
  })

  return promiseFromAWSRequest(req, 'jobId')
}

function doListJobs(job) {
  let glacier = getClient(job.region)
  req = glacier.listJobs({
    accountId: '-',
    vaultName: job.vaultName
  })

  return promiseFromAWSRequest(req, 'JobList')
}

function promiseFromAWSRequest(req, dataField) {

  console.log(promiseFromAWSRequest)

  req.send()

  return new Promise((resolve, reject) => {
    req.on('success', (res) => {
      resolve(res.data[dataField])
    }).on('error', (res) => {
      reject(res.error)
    })
  })

}

setUpChannelWithRetry((ch) => {
  console.log("Connected to RabbitMQ")
  ch.assertQueue(WORK_QUEUE)
  ch.prefetch(1)

  ch.consume(WORK_QUEUE, function(msg){

    let job = JSON.parse(msg.content.toString())
    console.log(job)

    handleJob(job).then((data) => {
      console.log(data)
      ch.sendToQueue(msg.properties.replyTo, Buffer.from(JSON.stringify(data)), {correlationId: msg.properties.correlationId})
    },(err) => {
      ch.sendToQueue(msg.properties.replyTo, Buffer.from("ERROR!!!"), {correlationId: msg.properties.correlationId})
    })
    
  }, {noAck: true})

}, (error) => {
  console.log(`Cannot connect to RabbitMQ, retrying in ${connectionRetryInterval} seconds...`)
}, connectionRetryInterval)