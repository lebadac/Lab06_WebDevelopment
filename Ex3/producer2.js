const amqp = require("amqplib");
const express = require("express");
const bodyParser = require("body-parser");
const { faker } = require("@faker-js/faker");

const app = express();
app.use(bodyParser.json());

const RABBITMQ_URL = "amqp://localhost";
const QUEUE = "messages";

let connection;
let channel;

// Function to generate fake messages
function generateFakeMessage() {
  return {
    id: faker.string.uuid(),
    name: faker.person.fullName(),
    email: faker.internet.email(),
    content: faker.lorem.paragraph(), // Longer content to differentiate from producer1
    timestamp: new Date().toISOString(),
    producer: "Producer2", // Identifies the source of the message
  };
}

// Function to initialize RabbitMQ
async function initRabbitMQ() {
  try {
    console.log("Connecting to RabbitMQ...");
    connection = await amqp.connect(RABBITMQ_URL);
    channel = await connection.createChannel();
    await channel.assertQueue(QUEUE);
    console.log("RabbitMQ connected and queue asserted.");
  } catch (error) {
    console.error("Error initializing RabbitMQ:", error);
    process.exit(1); // Exit if unable to connect
  }
}

// Function to send messages to RabbitMQ
async function sendMessage(message) {
  try {
    if (!channel) {
      throw new Error("Channel is not initialized");
    }
    channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(message)));
    console.log(`Message sent: ${JSON.stringify(message)}`);
  } catch (error) {
    console.error("Error sending message:", error);
  }
}

// Function to start auto-sending messages every 9 seconds
function startAutoProducer() {
  console.log("Starting auto-producer...");
  setInterval(async () => {
    const fakeMessage = generateFakeMessage();
    await sendMessage(fakeMessage);
  }, 9000); // Sends messages every 9 seconds (different from producer1)
}

// Function to close RabbitMQ connection
async function closeRabbitMQ() {
  try {
    console.log("Closing RabbitMQ connection...");
    if (channel) await channel.close();
    if (connection) await connection.close();
    console.log("RabbitMQ connection closed.");
  } catch (error) {
    console.error("Error closing RabbitMQ connection", error);
  }
}

// Handle graceful shutdown
process.on("SIGINT", async () => {
  await closeRabbitMQ();
  process.exit(0);
});

// Start the producer
const PORT = 3001; // Different port from producer1
app.listen(PORT, async () => {
  console.log(`Producer2 running on http://localhost:${PORT}`);
  await initRabbitMQ();
  startAutoProducer();
});
