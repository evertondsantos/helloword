using System;

namespace HelloWord.Model
{
    public class Message
    {
        public Guid Id { get; set; }
        public string MicroServico { get; set; }
        public string Content { get; set; }
        public DateTime Timestamp { get; set; }

    }
}