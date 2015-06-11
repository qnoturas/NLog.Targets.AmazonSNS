using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using Amazon;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;
using NLog.Common;
using NLog.Layouts;

namespace NLog.Targets.AmazonSNS
{
    [Target("SQSTarget")]
    public class SQSTarget : TargetWithLayout
    {
        private static readonly int DefaultMaxMessageSize = 262144;
        private static readonly string TruncateMessage = " [truncated]";
        private static readonly Encoding TransferEncoding = Encoding.UTF8;
        private int truncateSizeInBytes;

        private AmazonSQSConfig Config { get; set; }
        private AmazonSQSClient Client { get; set; }

        public string AwsAccessKey { get; set; }
        public string AwsSecretKey { get; set; }
        [DefaultValue("us-east-1")]
        public string Endpoint { get; set; }
        public string QueryUrl { get; set; }

        public int? MaxMessageSize { get; set; }
        public int ConfiguredMaxMessageSizeInBytes { get; private set; }

        [DefaultValue(0)]
        public int? DelaySeconds { get; set; }

        protected override void InitializeTarget()
        {
            base.InitializeTarget();
            
            var size = MaxMessageSize ?? DefaultMaxMessageSize;
            if (size <= 0 || size >= 256)
            {
                ConfiguredMaxMessageSizeInBytes = 256 * 1024;
            }
            else if (size <= DefaultMaxMessageSize)
            {
                ConfiguredMaxMessageSizeInBytes = DefaultMaxMessageSize * 1024;
            }
            else
            {
                ConfiguredMaxMessageSizeInBytes = size * 1024;
            }

            InternalLogger.Info(string.Format("Max message size is set to {0} KB.", ConfiguredMaxMessageSizeInBytes / 1024));
            truncateSizeInBytes = ConfiguredMaxMessageSizeInBytes - TransferEncoding.GetByteCount(TruncateMessage);

            Config = new AmazonSQSConfig
            {
                RegionEndpoint = RegionEndpoint.GetBySystemName(Endpoint)
            };

            try
            {
                if (string.IsNullOrEmpty(AwsAccessKey) && string.IsNullOrEmpty(AwsSecretKey))
                {
                    InternalLogger.Info("AWS Access Keys are not specified. Use Application Setting or EC2 Instance profile for keys.");
                    Client = new AmazonSQSClient(Config);
                }
                else
                {
                    Client = new AmazonSQSClient(AwsAccessKey, AwsSecretKey, Config);
                }
            }
            catch (Exception e)
            {
                InternalLogger.Fatal("Amazon SQS client failed to be configured. This logger wont'be send any message. Error is\n{0}\n{1}", e.Message, e.StackTrace);
            }
        }

        protected override void Write(LogEventInfo logEvent)
        {
            var logMessage = Layout.Render(logEvent);

            var count = Encoding.UTF8.GetByteCount(logMessage);
            if (count > ConfiguredMaxMessageSizeInBytes)
            {
                if (InternalLogger.IsWarnEnabled)
                    InternalLogger.Warn("logging message will be truncted. original message is\n{0}", logMessage);

                logMessage = logMessage.LeftB(TransferEncoding, truncateSizeInBytes) + TruncateMessage;
            }

            if (String.IsNullOrEmpty(QueryUrl))
            {
                if(InternalLogger.IsErrorEnabled)
                    InternalLogger.Error("QueryUrl is not defined.");

                return;
            }

            try
            {
                var message = new SendMessageRequest { MessageBody = logMessage, QueueUrl = QueryUrl, DelaySeconds = (DelaySeconds ?? 0) };
                Client.SendMessage(message);
            }
            catch (AmazonSQSException e)
            {
                InternalLogger.Fatal("RequstId: {0},ErrorType: {1}, Status: {2}\nFailed to send log with\n{3}\n{4}",
                    e.RequestId, e.ErrorType, e.StatusCode,
                    e.Message, e.StackTrace);
            }
        }
    }
}
