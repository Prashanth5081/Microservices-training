using RabbitMQ.Client;
using System.Text;
using System.Text.Json;

namespace DriverService.Messaging
{
    public interface IMessagePublisher
    {
        Task PublishAsync<T>(string exchangeName, string routingKey, T message);
    }

    public class RabbitMQPublisher : IMessagePublisher, IDisposable
    {
        private readonly ConnectionFactory _factory;
        private readonly SemaphoreSlim _lock = new(1, 1);
        private IConnection? _connection;
        private IChannel? _channel;
        private readonly ILogger<RabbitMQPublisher> _logger;

        public RabbitMQPublisher(IConfiguration configuration, ILogger<RabbitMQPublisher> logger)
        {
            _logger = logger;
            _factory = new ConnectionFactory()
            {
                HostName = configuration["RabbitMQ:HostName"] ?? "localhost",
                Port = int.Parse(configuration["RabbitMQ:Port"] ?? "5672"),
                UserName = configuration["RabbitMQ:UserName"] ?? "guest",
                Password = configuration["RabbitMQ:Password"] ?? "guest"
            };
        }

        private async Task<IChannel> GetChannelAsync()
        {
            if (_channel is { IsOpen: true }) return _channel;
            await _lock.WaitAsync();
            try
            {
                if (_channel is { IsOpen: true }) return _channel;
                _connection = await _factory.CreateConnectionAsync();
                _channel = await _connection.CreateChannelAsync();
                return _channel;
            }
            finally
            {
                _lock.Release();
            }
        }

         public async Task PublishAsync<T>(string exchangeName, string routingKey, T message)
        {
            try
            {
                var channel = await GetChannelAsync();
                await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Topic, durable: true);

                var jsonMessage = JsonSerializer.Serialize(message);
                var body = Encoding.UTF8.GetBytes(jsonMessage);

                await channel.BasicPublishAsync(
                    exchange: exchangeName,
                    routingKey: routingKey,
                    body: body);

                _logger.LogInformation($"Published message to {exchangeName}/{routingKey}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error publishing message to {exchangeName}/{routingKey}");
                throw;
            }
        }

        public void Dispose()
        {
            _channel?.Dispose();
            _connection?.Dispose();
        }

    }
}