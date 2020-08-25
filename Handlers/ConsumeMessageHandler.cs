using Confluent.Kafka;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;

namespace HelloWord.Handlers
{
    public class ConsumeMessageHandler : IHostedService
    {
        private readonly ILogger _logger;
        public ConsumeMessageHandler(ILogger<ConsumeMessageHandler> logger)
        {
            _logger = logger;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var conf = new ConsumerConfig
            {
                GroupId = "helloword",
                BootstrapServers = "localhost:9092"
            };

            using (var c = new ConsumerBuilder<Null, string>(conf).Build())
            {
                c.Subscribe("messagesHelloWord");
                var cts = new CancellationTokenSource();

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            _logger.LogInformation($"Mensagem: {cr.Message.Value} recebida de {cr.TopicPartitionOffset}");
                        }
                        catch (ConsumeException e)
                        {
                            _logger.LogError($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    c.Close();
                }
            }

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}