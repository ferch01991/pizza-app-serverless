const {v4:uuidv4} = require('uuid')
const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs")
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb")
const { DynamoDBDocumentClient, PutCommand, UpdateCommand, GetCommand } = require("@aws-sdk/lib-dynamodb")

// Create a SQS client
const sqsClient = new SQSClient({region: process.env.REGION})
const QUEUES = {
    pending: process.env.PENDING_ORDER_QUEUE,
    sending: process.env.SENDING_ORDER_QUEUE
}

// Create a DynamoDB client
const dynamodbClient = new DynamoDBClient({region: process.env.REGION})

// Create a DocumentDB client
const documentDBClient = DynamoDBDocumentClient.from(dynamodbClient)

exports.newOrder = async (event) => {
    const orderId = uuidv4();
    console.log(`orderID: ${orderId}`)

    let orderDetails

    try {
        orderDetails = JSON.parse(event.body)
    } catch (error) {
        console.log(`Order details bad formatter ${error}`)
        return {
            statusCode: 400,
            body: JSON.stringify({
                message: 'Invalid JSON format in order details'
            })
        }
    }

    console.log(`orderDetails: ${orderDetails}`)

    const order = {orderId, ...orderDetails}

    // Save order in DB
    await saveItemToDynamoDB(order)
    
    // Send message to SQS
    const response = await sendMessageToSQS(order, QUEUES.pending)
    console.log('Respuesta de la cola: ', response)

    return {
        statusCode: 200,
        body: JSON.stringify({
            message: order
        })
    }
}

exports.getOrder = async (event) => {
    console.log(event)
    
    try{
        const path = event.pathParameters
        console.log(path)
        const orderID = path.id
        console.log(orderID)

        if (orderID){
            const order = await getItemFromDynamoDB(orderID)
            return {
                statusCode: 200,
                body: JSON.stringify(order)
            }
        }else {
            return {
                statusCode: 424,
                body: JSON.stringify({
                    message: "Order ID is required"
                })
            }
        }


    }catch(error){
        if (error.name == "ItemNotFoundException"){
            return {
                statusCode: 404,
                body: JSON.stringify({
                    message: "Order not found"
                })
            }
        }
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: "Error retrieving order",
                error: error
            })
        }
    }


}

exports.prepOrder = async (event) => {
    console.log('prepOrder: ', event) 

    const body = JSON.parse(event.Records[0].body)
    const orderID = body.orderId
    await updateStatusInOrder(orderID, "COMPLETED")

    return
}

exports.sendOrder = async (event) => {
    console.log(event)

    if (event.Records[0].eventName == "MODIFY") {
        const eventBody = event.Records[0].dynamodb
        console.log('eventBody: ', eventBody)

        const orderDetails = eventBody.NewImage;
        console.log(orderDetails)

        const order = {
            orderId: orderDetails.orderId.S,
            pizza: orderDetails.pizza.S,
            client_id: orderDetails.client_id.S,
            order_status: orderDetails.order_status.S,
        }
        console.log(order)

        await sendMessageToSQS(order , QUEUES.sending)

    }

    
}

async function sendMessageToSQS(message, queue){
    const params = {
        QueueUrl: queue,
        MessageBody: JSON.stringify(message)
    }

    console.log(params)

    try {
        const command = new SendMessageCommand(params)
        const data = await sqsClient.send(command)
        
        return data
    } catch (error) {
        console.log(error)
        console.log(`Error sending message, ${data}`)
        throw error
    }
}

async function saveItemToDynamoDB(item){
    const params = {
        TableName: process.env.ORDERS_TABLE,
        Item: item
    }

    console.log('Params: ', params)

    try {
        const command = new PutCommand(params)
        const response = await documentDBClient.send(command)
        console.log('Response DB: ', response)
        return response
    } catch (error) {
        console.log('Error saving item: ', error)
        throw error
        
    }
}

async function updateStatusInOrder(orderId, status){
    const params = {
        TableName: process.env.ORDERS_TABLE,
        Key: {orderId},
        UpdateExpression: "SET order_status = :c",
        ExpressionAttributeValues: {
            ":c": status
        },
        ReturnValues: "ALL_NEW"
    }

    console.log(params)

    try {
        const command = new UpdateCommand(params)
        const response = await documentDBClient.send(command)
        console.log('Item update succesfully', response)
        return response
    } catch (error) {
        console.log('Error update Item', error)
        throw error
        
    }
}

async function getItemFromDynamoDB(orderId){
    const params = {
        TableName: process.env.ORDERS_TABLE,
        Key: {orderId},
    }

    console.log(params)

    try {
        const command = new GetCommand(params)
        const response = await documentDBClient.send(command)
        console.log('Respnse GET: ', response)
        if (response.Item){
            console.log('Item retrieved succesfully', response.Item)
            return response.Item
        }else{
            console.log('Item not found', response)
            let notFoundError = Error('Not found')
            notFoundError.name = "ItemNotFoundException"
            throw notFoundError
        }
    } catch (error) {
        console.log('Error get Item', error)
        throw error
    }
}