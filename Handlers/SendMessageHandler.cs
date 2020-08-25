using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using HelloWord.Model;

namespace HelloWord.Handlers
{
    public class SendMessageHandler : IHostedService
    {
        private readonly ILogger _logger;
        public SendMessageHandler(ILogger<SendMessageHandler> logger)
        {
            _logger = logger;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var name = $"Hello Word - {DateTime.Now.ToLongTimeString()}";
            Thread t = new Thread(() =>
            {
                while (true)
                {
                    SendMessage(name);
                    Thread.Sleep(5000);
                }
            });
            t.Start();
            return Task.CompletedTask;

        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;

        }

        private void SendMessage(string name)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var message = JsonConvert.SerializeObject(new Message
                    {
                        Content = "Hello World",
                        Id = Guid.NewGuid(),
                        Timestamp = DateTime.Now,
                        MicroServico = name,
                    });
                    var sendResult = producer.ProduceAsync("messagesHelloWord", new Message<Null, string> { Value = message }).GetAwaiter().GetResult();

                    _logger.LogInformation($"Mensagem '{sendResult.Value}' de '{sendResult.TopicPartitionOffset}'");

                }
                catch (ProduceException<Null, string> e)
                {
                    _logger.LogError($"Delivery failed: {e.Error.Reason}");
                }
            }
        }
    }
}