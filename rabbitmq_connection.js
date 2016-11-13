const amqp = require('amqplib')

function connectToRabbit(retryOnError){
  return amqp.connect('amqp://localhost').then((conn) => {

    // console.log("Connected to RabbitMQ.")

    conn.on('error', retryOnError)
    conn.on('close', retryOnError)

    return conn.createChannel().then((ch) => {
      // console.log("Channel established.")
      return ch
    }, (err) => {
     // console.log("Error establishing channel.")
     throw err
    })

  }, (err) => {
    // console.log("Error connecting to RabbitMQ.")
    throw err
  })
}

function setUpChannelWithRetry(onSuccess, onError, retryDelay) {
  let boundRetry = setUpChannelWithRetry.bind(null, onSuccess, onError, retryDelay)
  connectToRabbit(boundRetry).then((ch) => {
    onSuccess(ch)
  },(err) => { 
    setTimeout(boundRetry, retryDelay*1000)
    onError(err)
  })
}

function closeConnection(conn, delay) {
  return setTimeout(()=> {
    console.log(`Closing connection...`)
    conn.close()
  }, delay*1000)
}

module.exports = setUpChannelWithRetry