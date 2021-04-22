import { Kafka, logCreator, logLevel, Producer, ProducerBatch } from 'kafkajs'
import Configuration from './Configuration'

export default class ProducerFactory {
  private producer: Producer

  constructor(logger: logCreator) {
    this.producer = this.createProducer(logger)
  }

  public async start(): Promise<void> {
    try {
      await this.producer.connect()
    } catch (error) {
      console.log('Error connecting the producer: ', error)
    }
  }

  public async shutdown(): Promise<void> {
    await this.producer.disconnect()
  }

  public async sendBatch(batch: ProducerBatch): Promise<void> {
    await this.producer.sendBatch(batch)
  }

  private createProducer(logger: logCreator) : Producer {
    const kafka = new Kafka({
      clientId: Configuration.getGroupId(),
      brokers: Configuration.getBrokerList(),
      logLevel: logLevel.ERROR,
      logCreator: logger,
      connectionTimeout: 5000,
      authenticationTimeout: 2500,
      requestTimeout: 30000,
      ssl: {
        rejectUnauthorized: false
      },
      sasl: {
        mechanism: 'scram-sha-512',
        username: Configuration.getUsername(),
        password: Configuration.getPassword()
      },
      retry: {
        retries: 1000
      }
    })

    return kafka.producer()
  }
}
