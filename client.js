const amqp = require('amqplib');
const constants = require('./constants')
const WORK_QUEUE = constants['WORK_QUEUE']
const JOB_TYPES = constants['JOB_TYPES']
const uuid = require('uuid')
const Vorpal = require('vorpal')
const setUpChannelWithRetry = require('./rabbitmq_connection')
const CLIENT_CONSUMER_TAG = uuid.v4()
const connectionRetryInterval = 3

let REPLY_QUEUE = null
let CHANNEL = null

const vorpal = new Vorpal().delimiter('glacier$')

function withDefaultOption(command) {
  return command.option('-r, --region <region>', 'Glacier AWS region', ['us-west-2']).validate((args) => {
    if (CHANNEL != null) { return true; }
    return "Please wait for server connection to be re-established."
  })
}

function createCommand(cli, commandStr, callback) {
  withDefaultOption(cli.command(commandStr)).action(callback)
}

function listVaults(args, fin) {
  
  console.log('List Vaults')

  queueJob({
    region: args.options.region || 'us-west-2',
    type: JOB_TYPES.LIST_VAULTS
  }, (msg) => {
    printVaultInfo(msg)
    fin()
  })
}

createCommand(vorpal, 'lv', listVaults)

function vaultInventory(args, fin) {
  if (args.vaultName == null) {
    console.log("Please specify vault name for this operation.")
  }

  console.log(`Vault Inventory for ${args.vaultName}`)

  queueJob({
    region: args.options.region || 'us-west-2',
    vaultName: args.vaultName,
    type: JOB_TYPES.VAULT_INVENTORY
  }, (msg) => {
    console.log(`Vault inventory job has been queued with ID ${msg}`)
    fin()
  }) 
}

createCommand(vorpal, 'vi <vaultName>', vaultInventory)

function listJobs(args, fin) {
  if (args.vaultName == null) {
    console.log("Please specify vault name for this operation.")
  }

  console.log(`List jobs for vault ${args.vaultName}`)

  queueJob({
    region: args.options.region || 'us-west-2',
    vaultName: args.vaultName,
    type: JOB_TYPES.LIST_JOBS
  }, (msg) => {
    console.log(msg)
    logCommandSeperator()
    fin()
  })     
}

createCommand(vorpal, 'lj <vaultName>', listJobs)











function queueJob(job, done){
  let ch = CHANNEL
  setUpQueues(ch).then((q) => {
    
    console.log(`Sending request to server...\n`)

    ch.sendToQueue(WORK_QUEUE, serializeJSONMessage(job),
      {replyTo: q, correlationId: uuid.v4()})
    
    ch.consume(q, (msg) => {
      ch.cancel(CLIENT_CONSUMER_TAG)
      done(deserializeJSONMessage(msg))
    }, {noAck: true, consumerTag: CLIENT_CONSUMER_TAG})

  })
}

function setUpQueues(ch){
  assertWorkQueuePromise = ch.assertQueue(WORK_QUEUE)

  if (REPLY_QUEUE == null) {
    assertReplyQueuePromise = ch.assertQueue('', {exclusive: true}).then((ok) => {
      return ok.queue
    })
  } else {
    assertReplyQueuePromise = new Promise((resolve, _) => {
      resolve(REPLY_QUEUE)
    })
  }

  return assertWorkQueuePromise.then(() => {
    return assertReplyQueuePromise
  })
}

function printVaultInfo(vaultList) {
  if(vaultList && vaultList.length > 0){
    var props = [['VaultARN','ARN'], 
    ['VaultName','Name'], ['CreationDate', 'Created On'], 
    ['LastInventoryDate', 'Last Inventory On'], 
    ['NumberOfArchives', '# Archives'], ['SizeInBytes', 'Size (MB)']]
    
    vaultList.forEach(function(vault){
      vault['SizeInBytes'] = Math.round(vault['SizeInBytes']/1048576*100)/100
      printProps(props, vault)
    })
    return
  }

  console.log("There is no vault")
}

function printProps(propsLabels, content){
  propsLabels.forEach(function(keyLabel){
    console.log( `${keyLabel[1]}: ${content[keyLabel[0]]}`)
  })
  logSeperator()
}

function logSeperator(){
  console.log('----------------------\n')
}

function logCommandSeperator(){
  console.log('----------------------\n')
  console.log("Please enter new command or type `exit` to quit\n")
}

function serializeJSONMessage(msg) {
  return Buffer.from(JSON.stringify(msg))
}

function deserializeJSONMessage(msg) {
  return JSON.parse(msg.content.toString())
}

setUpChannelWithRetry((ch) => {
  CHANNEL = ch
  console.log("Connected to RabbitMQ.")
  vorpal.show()
}, (error) => {
  CHANNEL = null
  console.log(`Cannot connect to RabbitMQ, retrying in ${connectionRetryInterval} seconds...`)
}, connectionRetryInterval)