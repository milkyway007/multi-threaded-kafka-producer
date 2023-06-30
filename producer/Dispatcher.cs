using Confluent.Kafka;
using log4net;

namespace producer
{
    internal class Dispatcher
    {
        private static readonly ILog _log = LogManager.GetLogger("default");
        private readonly string _fileLocation;
        private readonly string _topicName;
        private readonly IProducer<int?, string> _producer;

        public Dispatcher(string fileLocation, string topicName, IProducer<int?, string> producer)
        {
            _fileLocation = fileLocation;
            _topicName = topicName;
            _producer = producer;
        }

        public async Task Run()
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
                        _ = await _producer.ProduceAsync(
                        _topicName,
                        new Message<int?, string>
                        {
                            Key = null,
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
