using Confluent.Kafka;
using log4net;

namespace producer
{
    internal class Dispatcher
    {
        private static readonly ILog _log = LogManager.GetLogger("default");
        private readonly string _fileLocation;
        private readonly string _topicName;
        private readonly IProducer<int, string> _producer;

        public Dispatcher(IProducer<int, string> producer, string fileLocation, string topicName)
        {
            _fileLocation = fileLocation;
            _topicName = topicName;
            _producer = producer;
        }

        public void Run()
        {
            _log.Info($"Start Processing {_fileLocation}");
            int counter = 0;
            try
            {
                using StreamReader reader = new(_fileLocation);
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    if (line != null)
                    {
                        _producer.Produce(
                        _topicName,
                        new Message<int, string>
                        {
                            Key = 0,
                            Value = line,
                        });
                        counter++;
                    }
                }
                _log.Info($"Finished Sending {counter} messages from {_fileLocation}");

            } catch (FileNotFoundException e)
            {
                throw new InvalidOperationException(e.Message);
            }
        }
    }
}