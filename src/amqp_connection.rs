use futures::{Future, StreamExt};
use lapin::{
    message::Delivery,
    options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    publisher_confirm::Confirmation,
    types::FieldTable,
    uri::AMQPUri as AmqpUri,
    BasicProperties, Channel, Connection, Consumer, Queue,
};
use std::str::FromStr;
use tokio::task::JoinHandle;

/// Since this example uses a single queue, just make its name an accessible constant.
const QUEUE_NAME: &str = "hello";

pub struct AmqpConnection {
    _connection: Connection,
    channel: Channel,
    _queue: Queue,
    _consumer_task: JoinHandle<()>,
}

impl AmqpConnection {
    pub async fn new<DeliveryHandlerFuture, DeliveryHandler>(
        server_uri: &str,
        mut handle_delivery: DeliveryHandler,
    ) -> Result<Self, Error>
    where
        DeliveryHandlerFuture: Future<Output = ()> + Send,
        DeliveryHandler: 'static + Send + FnMut(lapin::Result<Delivery>) -> DeliveryHandlerFuture,
    {
        let server_uri = AmqpUri::from_str(server_uri)
            .map_err(UriError)
            .map_err(Error::BadUri)?;

        let locale = String::from("en_GB");
        let connection = connect_to_server(server_uri, locale)
            .await
            .map_err(Error::Connection)?;

        let channel = create_channel(&connection).await.map_err(Error::Channel)?;

        let queue = declare_queue(&channel, QUEUE_NAME)
            .await
            .map_err(Error::Queue)?;

        let mut consumer = create_consumer(&channel).await.map_err(Error::Consumer)?;

        let consumer_task = tokio::spawn(async move {
            while let Some(delivery) = consumer.next().await {
                handle_delivery(delivery).await;
            }
        });

        Ok(Self {
            _connection: connection,
            channel,
            _queue: queue,
            _consumer_task: consumer_task,
        })
    }

    pub async fn publish(&self, payload: &[u8]) -> lapin::Result<Confirmation> {
        let options = BasicPublishOptions::default();
        let properties = BasicProperties::default();

        // TODO: Not sure if this is the right type interface for this function. Should lapin's
        // types get wrapped?
        self.channel
            .basic_publish("", QUEUE_NAME, options, payload, properties)
            .await?
            .await
    }
}

async fn connect_to_server(uri: AmqpUri, locale: String) -> lapin::Result<Connection> {
    let options = lapin::ConnectionProperties {
        locale,
        ..Default::default()
    };

    lapin::Connection::connect_uri(uri, options).await
}

async fn create_channel(connection: &lapin::Connection) -> lapin::Result<Channel> {
    connection.create_channel().await
}

async fn declare_queue(channel: &lapin::Channel, name: &str) -> lapin::Result<Queue> {
    let options = QueueDeclareOptions::default();
    let argument = FieldTable::default();
    channel.queue_declare(name, options, argument).await
}

async fn create_consumer(channel: &lapin::Channel) -> lapin::Result<Consumer> {
    let tag = "my_consumer";
    let options = BasicConsumeOptions::default();
    let argument = FieldTable::default();

    channel
        .basic_consume(QUEUE_NAME, tag, options, argument)
        .await
}

/// An error which may cause [`AmqpConnection::new`] to fail.
#[derive(Debug)]
pub enum Error {
    BadUri(UriError),
    Connection(lapin::Error),
    Channel(lapin::Error),
    Queue(lapin::Error),
    Consumer(lapin::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::BadUri(_) => f.write_str("invalid uri"),
            Error::Connection(_) => f.write_str("failed to connect to server"),
            Error::Channel(_) => f.write_str("failed to create channel"),
            Error::Queue(_) => f.write_str("failed to declare queue"),
            Error::Consumer(_) => f.write_str("failed to create consumer"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::BadUri(source) => Some(source),
            Error::Connection(source) => Some(source),
            Error::Channel(source) => Some(source),
            Error::Queue(source) => Some(source),
            Error::Consumer(source) => Some(source),
        }
    }
}

#[derive(Debug)]
pub struct UriError(String);

impl std::fmt::Display for UriError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for UriError {}
