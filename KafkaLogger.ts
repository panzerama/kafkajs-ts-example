import { logCreator, logLevel } from 'kafkajs'

export default class KafkaLogger {
  public logCreator(): logCreator {
    return () => ({ namespace, label, log }) => {
      const { timestamp, logger, message, ...others } = log
      const errorMessage = `${label} [${namespace}] ${message} ${JSON.stringify(others)}`

      console.log(errorMessage)
    }
  }
}
