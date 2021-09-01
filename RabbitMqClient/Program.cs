using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMqClient
{
    class Program
    {
        static IConnectionFactory factory = new ConnectionFactory()
        {
            HostName = "127.0.0.1",
            UserName = "admin",
            Password = "admin",
            VirtualHost = "/"
        };
        static void Main(string[] args)
        {
            Console.WriteLine("Hello Producer!");
            SendMessage_delay();
            Console.ReadLine();
        }

        public static void SendMessage()
        {
            //死信交换机
            string dlxexChange = "dlx.exchange";
            //死信队列
            string dlxQueueName = "dlx.queue";

            //消息交换机
            string exchange = "direct-exchange";
            //消息队列
            string queueName = "queue_a";

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {

                    //创建死信交换机
                    channel.ExchangeDeclare(dlxexChange, type: ExchangeType.Direct, durable: true, autoDelete: false);
                    //创建死信队列
                    channel.QueueDeclare(dlxQueueName, durable: true, exclusive: false, autoDelete: false);
                    //死信队列绑定死信交换机
                    channel.QueueBind(dlxQueueName, dlxexChange, routingKey: dlxQueueName);

                    // 创建消息交换机
                    channel.ExchangeDeclare(exchange, type: ExchangeType.Direct, durable: true, autoDelete: false);
                    //创建消息队列,并指定死信队列
                    channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments:
                                        new Dictionary<string, object> {
                                             { "x-dead-letter-exchange",dlxexChange}, //设置当前队列的DLX(死信交换机)
                                             { "x-dead-letter-routing-key",dlxQueueName}, //设置DLX的路由key，DLX会根据该值去找到死信消息存放的队列
                                        });
                    //消息队列绑定消息交换机
                    channel.QueueBind(queueName, exchange, routingKey: queueName);

                    string message = "hello rabbitmq message";
                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true;
                    //发布消息
                    channel.BasicPublish(exchange: exchange,
                                         routingKey: queueName,
                                         basicProperties: properties,
                                         body: Encoding.UTF8.GetBytes(message));
                    Console.WriteLine($"{DateTime.Now}向队列:{queueName}发送消息:{message}");
                }
            }
        }

        public static void SendMessage_delay()
        {
            //死信交换机
            string dlxexChange = "dlx.exchange";
            //死信队列
            string dlxQueueName = "dlx.queue";

            //消息交换机
            string exchange = "direct-exchange";
            //消息队列
            string queueName = "queue_a";

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {

                    //创建死信交换机
                    channel.ExchangeDeclare(dlxexChange, type: ExchangeType.Direct, durable: true, autoDelete: false);
                    //创建死信队列
                    channel.QueueDeclare(dlxQueueName, durable: true, exclusive: false, autoDelete: false);
                    //死信队列绑定死信交换机
                    channel.QueueBind(dlxQueueName, dlxexChange, routingKey: dlxQueueName);

                    // 创建消息交换机
                    channel.ExchangeDeclare(exchange, type: ExchangeType.Direct, durable: true, autoDelete: false);
                    //创建消息队列,并指定死信队列
                    channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments:
                                        new Dictionary<string, object> {
                                             { "x-dead-letter-exchange",dlxexChange}, //设置当前队列的DLX(死信交换机)
                                             { "x-dead-letter-routing-key",dlxQueueName}, //设置DLX的路由key，DLX会根据该值去找到死信消息存放的队列
                                        });
                    //消息队列绑定消息交换机
                    channel.QueueBind(queueName, exchange, routingKey: queueName);

                    string message = "hello rabbitmq message 10s后处理";
                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true;
                    properties.Expiration = "10000";
                    //发布消息
                    channel.BasicPublish(exchange: exchange,
                                     routingKey: queueName,
                                     basicProperties: properties,
                                     body: Encoding.UTF8.GetBytes(message));
                    Console.WriteLine($"{DateTime.Now}向队列:{queueName}发送消息:{message}");


                    message = "hello rabbitmq message 5s后处理";
                    properties = channel.CreateBasicProperties();
                    properties.Persistent = true;
                    properties.Expiration = "5000";
                    channel.BasicPublish(exchange: exchange,
                                    routingKey: queueName,
                                    basicProperties: properties,
                                    body: Encoding.UTF8.GetBytes(message));
                    Console.WriteLine($"{DateTime.Now}向队列:{queueName}发送消息:{message}");

                }
            }
        }
    }
}
