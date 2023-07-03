using Confluent.Kafka;
using log4net;

namespace producer
{
    internal class DispatcherDemo
    {
        private static readonly ILog _log = LogManager.GetLogger("default");

        public static void Demo()
        {
            var producerConfig = new ProducerConfig
            {
                ClientId = AppConfigs.APPLICATION_ID,
            };

            try
            {
                using StreamReader reader = new(AppConfigs.KAFKA_CONFIG_FILE_LOCATION);
                while (!reader.EndOfStream)
                {
                    var fileLine = reader.ReadLine();
                    if (fileLine != null)
                    {
                        var configValue = fileLine.Split('=');
                        producerConfig.Set(configValue[0], configValue[1]);
                    }
                }
            }
            catch(IOException ex)
            {
                throw new InvalidOperationException(ex.Message);
            }

            var producer = new ProducerBuilder<int?, string>(producerConfig).Build();
            Thread[] dispatchers = new Thread[AppConfigs.EVENT_FILES.Length];

            _log.Info("Starting Dispatcher threads...");

            for (int i = 0; i < AppConfigs.EVENT_FILES.Length; i++)
            {
                dispatchers[i] = new Thread(new ThreadStart(new Dispatcher(producer, AppConfigs.EVENT_FILES[i], AppConfigs.TOPIC_NAME).Run));
                dispatchers[i].Start();
            }

            try
            {
                foreach (Thread t in dispatchers)
                {
                    t.Join();
                }
            }
            catch (ThreadInterruptedException)
            {
                _log.Error("Main Thread Interrupted");
            }
            finally
            {
                producer.Dispose();
                _log.Info("Finished Dispatcher Demo");
            }
        }
    }
}
