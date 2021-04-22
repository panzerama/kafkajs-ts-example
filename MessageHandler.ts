import { Message, ProducerBatch, TopicMessages } from 'kafkajs'
import { CustomMessageFormat } from './MessageProcessor'
import Producer from './Producer'

export default class MessageHandler {
  private producer: Producer

  constructor(producer: Producer) {
    this.producer = producer
  }
  public async handle(message: CustomMessageFormat): Promise<void> {
    this.handleMany([message])
  }

  public async handleMany(messages: Array<CustomMessageFormat>): Promise<void> {
    const kafkaMessages: Array<Message> = messages.map((customMessage) => {
      return {
        value: JSON.stringify(customMessage)
      }
    })

    const topicMessages: TopicMessages = {
      topic: 'producer-topic',
      messages: kafkaMessages
    }

    const batch: ProducerBatch = {
      topicMessages: [topicMessages]
    }

    this.producer.sendBatch(batch)
  }
}