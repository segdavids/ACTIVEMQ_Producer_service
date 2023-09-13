using Apache.NMS.Util;
using Apache.NMS;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ActiveMQ_Producer.BLL
{
    public class BL
    {
        public static void GetMessages()
        {
            //string? ACTIVEMQ_TRANSPORT = Environment.GetEnvironmentVariable("ACTIVEMQ_TRANSPORT");  //"10.192.168.91";// 
            //string? ACTIVEMQ_PRIMARY_URL = Environment.GetEnvironmentVariable("ACTIVEMQ_PRIMARY_URL"); //"root";// 
            //string? ACTIVEMQ_OPEN_WIRE_PORT = Environment.GetEnvironmentVariable("ACTIVEMQ_OPEN_WIRE_PORT");// "1";//
            //string? ACTIVEMQ_USERNAME = Environment.GetEnvironmentVariable("ACTIVEMQ_USERNAME");  //"10.192.168.91";// 
            //string? ACTIVEMQ_PASSWORD = Environment.GetEnvironmentVariable("ACTIVEMQ_PASSWORD"); //"root";// 
            //string? ACTIVEMQ_TIMEOUT = Environment.GetEnvironmentVariable("ACTIVEMQ_TIMEOUT");// "1";//
            //string? TOPIC_NAME = Environment.GetEnvironmentVariable("TOPIC_NAME");
            string? BATCH_SIZE = Environment.GetEnvironmentVariable("BATCH_SIZE");


            //DEV
            string? ACTIVEMQ_TRANSPORT = "tcp";// Environment.GetEnvironmentVariable("ACTIVEMQ_TRANSPORT");  //"10.192.168.91";// 
            string? ACTIVEMQ_PRIMARY_URL = "DAVID-PC";// Environment.GetEnvironmentVariable("ACTIVEMQ_PRIMARY_URL"); //"root";// 
            int ACTIVEMQ_OPEN_WIRE_PORT = 61616; // Environment.GetEnvironmentVariable("ACTIVEMQ_OPEN_WIRE_PORT");// "1";//
            string? ACTIVEMQ_USERNAME = Environment.GetEnvironmentVariable("ACTIVEMQ_USERNAME");  //"10.192.168.91";// 
            string? ACTIVEMQ_PASSWORD = Environment.GetEnvironmentVariable("ACTIVEMQ_PASSWORD"); //"root";// 
            string? ACTIVEMQ_TIMEOUT = Environment.GetEnvironmentVariable("ACTIVEMQ_TIMEOUT");// "1";//
            string? TOPIC_NAME = "DAVID_TEST";// Environment.GetEnvironmentVariable("TOPIC_NAME");
            string? JSON_PATH = "C:\\Users\\Ajigbotoluwa O.David\\Documents\\EF\\FNB\\WFM\\FTCI-STCI\\FTCI-Connector_SpringBoot\\state-events-logger\\state-change.json";


            try
            {
                Logger($"Service starting...");
                Uri connecturi = new Uri($"{ACTIVEMQ_TRANSPORT}://{ACTIVEMQ_PRIMARY_URL}:{ACTIVEMQ_OPEN_WIRE_PORT}");
              //  Uri connecturi = new Uri("tcp://DAVID-PC:61616");
                Logger($"About to connect to {connecturi}");

                IConnectionFactory factory = new NMSConnectionFactory(connecturi);

                using (IConnection conn = factory.CreateConnection())
                using (ISession session = conn.CreateSession())
                {
                    IDestination destination = SessionUtil.GetDestination(session, $"topic://{TOPIC_NAME}");  // session.GetTopic(TOPIC_NAME);
                    Logger($"Destination is {destination}");

                    //CREATE CONSUMER
                    using (IMessageConsumer consumer = session.CreateConsumer(destination))
                    using (IMessageProducer producer = session.CreateProducer(destination))
                    {
                        conn.Start();

                        producer.DeliveryMode = MsgDeliveryMode.Persistent;

                        var jsontxt = File.ReadAllText(JSON_PATH);
                        // Send a message
                        ITextMessage request = session.CreateTextMessage(jsontxt); // "Hello World!");
                        request.NMSCorrelationID = "abc";
                        request.Properties["NMSXGroupID"] = "cheese";
                        request.Properties["myHeader"] = "Cheddar";

                        producer.Send(request);

                        
                    }
                }

            }
            catch (Exception ex)
            {
                Logger($"error: {ex}");
            }
        }

        public static void Logger(string message)
        {
            var timezonetime = DateTime.Now.ToUniversalTime(); //.ToUniversalTime(); //.AddHours(2);
            Console.WriteLine($"{timezonetime}: {message}");
            string logpath = Environment.GetEnvironmentVariable("LOG_PATH");//"C:\\EF\\"; //
            if (!string.IsNullOrEmpty(logpath))
            {
                string reportpath = $"{logpath}{timezonetime:dd_MM_yyyy}_log.txt";
                using (StreamWriter logitwriter = new(reportpath, true))
                {
                    logitwriter.WriteLine($"{timezonetime}: {message}");
                }
            }
        }
    }
}
