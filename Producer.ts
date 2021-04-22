import { Kafka, logCreator, logLevel, Producer, ProducerBatch } from 'kafkajs'

const clientId = 'producer-client'
const brokerList = ['localhost:9092']

export default class ProducerFactory {
  private producer: Producer

  constructor() {
    this.producer = this.createProducer()
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

  private createProducer() : Producer {
    const kafka = new Kafka({
      clientId: clientId,
      brokers: brokerList,
    })

    return kafka.producer()
  }
}
