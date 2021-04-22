import { EachBatchPayload, EachMessagePayload, IHeaders } from 'kafkajs'
import MessageHandler from './MessageHandler'

export interface CustomMessageFormat {
  payload?: CustomMessagePayload
  identifier: string
  headers: IHeaders
}

export interface CustomMessagePayload {
  a: string
}

export default class MessageProcessor {
  private messageHandler: MessageHandler

  constructor(messageHandler: MessageHandler) {
    this.messageHandler = messageHandler
  }

  public async processMessage(eachMessagePayload: EachMessagePayload): Promise<void> {
    const { topic, partition, message } = eachMessagePayload
    let processedMessage: CustomMessageFormat

    try {
      const value = Buffer.from(message.value.toString()).toString()
      const payload = JSON.parse(value) as CustomMessagePayload
      const headers: IHeaders = message.headers ?? {}
      processedMessage = {
        payload: payload,
        identifier: message.timestamp,
        headers: headers
      }
      await this.messageHandler.handle(processedMessage)
    } catch (error) {
      console.log(`Error processing message: ${error}`)
    }
  }

  public async processInBatch(eachBatchPayload: EachBatchPayload): Promise<void> {
    const { batch, resolveOffset, heartbeat, isRunning, isStale } = eachBatchPayload

    if (!isRunning() || isStale()) return

    const processedMessages: CustomMessageFormat[] = []

    for (const message of batch.messages) {
      if (message.value) {
        try {
          const value = Buffer.from(message.value.toString()).toString()
          const payload = JSON.parse(value) as CustomMessagePayload
          const headers: IHeaders = message.headers ?? {}
          processedMessages.push({
            payload: payload,
            identifier: message.timestamp,
            headers: headers
          })
        } catch (error) {
          console.log(`Error processing message: ${error}`)
        }
      }
      resolveOffset(message.offset)
    }

    heartbeat().catch((error) => {
      console.log(`Error sending heartbeat: ${error}`)
    })

    await this.messageHandler.handleMany(processedMessages)
  }
}
