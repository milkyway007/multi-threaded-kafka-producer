﻿namespace producer
{
    internal class AppConfig
    {
        public const string APPLICATION_ID = "multi-threaded-producer";
        public const string TOPIC_NAME = "nse-eod-topic";
        public const string KAFKA_CONFIG_FILE_LOCATION = "localhost: 9092,localhost: 9093";
        public static readonly string[] EVENT_FILES = new string[] { "data/NSE05NOV2018BHAV.csv", "data/NSE06NOV2018BHAV.csv" };
    }
}
