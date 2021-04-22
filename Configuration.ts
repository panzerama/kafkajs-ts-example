export default class Configuration {
  public static getConsumerTopicName(): string {
    return 'some-topic'
  }

  public static getStartOffsetAtBeginning(): boolean {
    return false
  }

  public static getUsername(): string {
    return 'user'
  }

  public static getPassword(): string {
    return 'pass'
  }

  public static getClientId(): string {
    return 'clientId'
  }

  public static getBrokerList(): Array<string> {
    return ['localhost:9092']
  }

  public static getGroupId(): string {
    return 'groupId'
  }

  public static getProducerTopicName(): string {
    return 'producerTopic'
  } 
}