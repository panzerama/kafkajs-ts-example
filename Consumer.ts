import { Consumer, ConsumerConfig, ConsumerSubscribeTopic, EachBatchPayload, Kafka, KafkaConfig, logLevel, SASLOptions, logCreator, EachMessagePayload } from 'kafkajs'
import Configuration from './Configuration'
import MessageProcessor from './MessageProcessor'

export default class ConsumerFactory {
  private kafkaConsumer: Consumer
  private messageProcessor: MessageProcessor

  public constructor(messageProcessor: MessageProcessor, logger: logCreator) {
    this.messageProcessor = messageProcessor
    this.kafkaConsumer = this.createKafkaConsumer(logger)
  }

  public async startConsumer(): Promise<void> {
    const topic: ConsumerSubscribeTopic = {
      topic: Configuration.getConsumerTopicName(),
      fromBeginning: Configuration.getStartOffsetAtBeginning()
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
      topic: Configuration.getConsumerTopicName(),
      fromBeginning: Configuration.getStartOffsetAtBeginning()
    }

    try {
      await this.kafkaConsumer.connect()
      await this.kafkaConsumer.subscribe(topic)

      await this.kafkaConsumer.run({
        partitionsConsumedConcurrently: 3,
        eachBatchAutoResolve: true,
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

  private createKafkaConsumer(logger: logCreator): Consumer {
    const saslOptions: SASLOptions = {
      mechanism: 'scram-sha-512',
      username: Configuration.getUsername(),
      password: Configuration.getPassword()
    }

    const kafkaConfig: KafkaConfig = {
      clientId: Configuration.getClientId(),
      brokers: Configuration.getBrokerList(),
      sasl: saslOptions,
      logLevel: logLevel.ERROR,
      logCreator: logger,
      connectionTimeout: 5000,
      authenticationTimeout: 2500,
      retry: {
        retries: 1000
      },
      requestTimeout: 60000,
      ssl: {
        rejectUnauthorized: false
      }
    }

    const kafka = new Kafka(kafkaConfig)

    const consumerConfig: ConsumerConfig = {
      groupId: Configuration.getGroupId(),
      sessionTimeout: 60000,
      heartbeatInterval: 20000
    }

    const consumer = kafka.consumer(consumerConfig)

    return consumer
  }
}
