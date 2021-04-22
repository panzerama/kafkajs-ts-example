import { Consumer, ConsumerConfig, ConsumerSubscribeTopic, EachBatchPayload, Kafka, KafkaConfig, logLevel, SASLOptions, logCreator, EachMessagePayload } from 'kafkajs'
import MessageProcessor from './MessageProcessor'

const topicName = 'consumer-topic'
const clientId = 'consumer-client'
const brokerList = ['localhost:9092']
export default class ConsumerFactory {
  private kafkaConsumer: Consumer
  private messageProcessor: MessageProcessor

  public constructor(messageProcessor: MessageProcessor) {
    this.messageProcessor = messageProcessor
    this.kafkaConsumer = this.createKafkaConsumer()
  }

  public async startConsumer(): Promise<void> {
    const topic: ConsumerSubscribeTopic = {
      topic: topicName,
      fromBeginning: false
    }

    try {
      await this.kafkaConsumer.connect()
      await this.kafkaConsumer.subscribe(topic)

      await this.kafkaConsumer.run({
        eachMessage: async (message: EachMessagePayload) => {
          await this.messageProcessor.processMessage(message)
        }
      })
    } catch (error) {
      console.log('Error: ', error)
    }
  }

  public async startBatchConsumer(): Promise<void> {
    const topic: ConsumerSubscribeTopic = {
      topic: topicName,
      fromBeginning: false
    }

    try {
      await this.kafkaConsumer.connect()
      await this.kafkaConsumer.subscribe(topic)

      await this.kafkaConsumer.run({
        partitionsConsumedConcurrently: 3,
        eachBatch: async (eatchBatchPayload: EachBatchPayload) => {
          await this.messageProcessor.processInBatch(eatchBatchPayload)
        }
      })
    } catch (error) {
      console.log('Error: ', error)
    }
  }

  public async shutdown(): Promise<void> {
    await this.kafkaConsumer.disconnect()
  }

  private createKafkaConsumer(): Consumer {
    const kafka = new Kafka({ 
      clientId: clientId,
      brokers: brokerList
    })
    const consumer = kafka.consumer({ groupId: 'consumer-group' })

    return consumer
  }
}
