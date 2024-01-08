use futures::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties,
};
use std::process::ExitCode;

async fn connect_to_amqp_server() -> Result<lapin::Connection, Error> {
    // Create connection to the AMQP server.
    let server_uri = "amqp://localhost:5672";
    let options = lapin::ConnectionProperties {
        locale: "en_GB".into(),
        ..Default::default()
    };

    let result = lapin::Connection::connect(server_uri, options).await;

    result.map_err(Error::Connection)
}

async fn create_amqp_channel(connection: &lapin::Connection) -> Result<lapin::Channel, Error> {
    let result = connection.create_channel().await;

    result.map_err(Error::AmqpSetup)
}

async fn declare_amqp_queue(channel: &lapin::Channel, name: &str) -> Result<lapin::Queue, Error> {
    let options = QueueDeclareOptions::default();
    let argument = FieldTable::default();
    let result = channel.queue_declare(name, options, argument).await;

    result.map_err(Error::AmqpSetup)
}

async fn create_amqp_consumer(
    channel: &lapin::Channel,
    queue_name: &str,
) -> Result<lapin::Consumer, Error> {
    let tag = "my_consumer";
    let options = BasicConsumeOptions::default();
    let argument = FieldTable::default();

    let result = channel
        .basic_consume(queue_name, tag, options, argument)
        .await;

    result.map_err(Error::AmqpSetup)
}

async fn run_producer(channel: lapin::Channel, queue_name: &str) -> Result<(), Error> {
    let payload = b"Hello, world!";

    loop {
        let options = BasicPublishOptions::default();
        let properties = BasicProperties::default();

        let publish_confirm = channel
            .basic_publish("", queue_name, options, payload, properties)
            .await
            .map_err(Error::Publish)?;

        publish_confirm.await.map_err(Error::Publish)?;

        println!("publish: {payload:?}");
    }
}

async fn run_consumer(mut consumer: lapin::Consumer) -> Result<(), Error> {
    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.map_err(Error::ConsumerDelivery)?;

        let ack_options = BasicAckOptions::default();
        delivery
            .ack(ack_options)
            .await
            .map_err(Error::ConsumerAck)?;

        println!("delivery: {delivery:?}");
    }

    Ok(())
}

async fn run_async() -> Result<(), Error> {
    let queue_name = "hello";

    // AMQP Setup
    let connection = connect_to_amqp_server().await?;
    let channel = create_amqp_channel(&connection).await?;
    let _queue = declare_amqp_queue(&channel, queue_name).await?;
    let consumer = create_amqp_consumer(&channel, queue_name).await?;

    // Run consumer & producer
    let consumer = run_consumer(consumer);
    let producer = run_producer(channel, queue_name);
    let result = futures::future::join(consumer, producer).await;

    // Report errors from consumer/producer functions
    result.0.or(result.1)
}

fn run() -> Result<(), Error> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .build()
        .map_err(Error::Runtime)?;

    runtime.block_on(run_async())
}

fn main() -> ExitCode {
    if let Err(e) = run() {
        print_error(&e);
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

fn print_error(mut error: &dyn std::error::Error) {
    eprintln!("{error}");

    while let Some(source) = error.source() {
        eprintln!("caused by:\n    {source}");
        error = source;
    }
}

#[derive(Debug)]
enum Error {
    Runtime(std::io::Error),
    Connection(lapin::Error),
    AmqpSetup(lapin::Error),
    ConsumerDelivery(lapin::Error),
    ConsumerAck(lapin::Error),
    Publish(lapin::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Runtime(_) => f.write_str("failed to create async runtime"),
            Error::Connection(_) => f.write_str("failed to connect to AMQP server"),
            Error::AmqpSetup(_) => f.write_str("failed to set up connection to AMQP server"),
            Error::ConsumerDelivery(_) => {
                f.write_str("failed to receive delivery for AMQP consumer")
            }
            Error::ConsumerAck(_) => f.write_str("failed to ACK an AMQP delivery"),
            Error::Publish(_) => f.write_str("failed to publish an AMQP message"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Runtime(source) => Some(source),
            Error::Connection(source) => Some(source),
            Error::AmqpSetup(source) => Some(source),
            Error::ConsumerDelivery(source) => Some(source),
            Error::ConsumerAck(source) => Some(source),
            Error::Publish(source) => Some(source),
        }
    }
}
