mod amqp_connection;

use crate::amqp_connection::AmqpConnection;
use lapin::{message::Delivery, options::BasicAckOptions};
use std::process::ExitCode;

async fn handle_consumer_delivery(delivery: lapin::Result<Delivery>) {
    match delivery {
        Ok(delivery) => {
            let ack_result = delivery.ack(BasicAckOptions::default()).await;

            match ack_result {
                Ok(()) => {
                    println!("received & ack'd delivery: {delivery:?}");
                }

                Err(error) => {
                    println!("received delivery: {delivery:?}. ack failed: {error}")
                }
            }
        }

        Err(error) => println!("delivery failed. error: {error}"),
    }
}

async fn run_async() -> Result<(), Error> {
    const SERVER_URI: &str = "amqp://localhost:5672";
    let connection = AmqpConnection::new(SERVER_URI, handle_consumer_delivery)
        .await
        .map_err(Error::Connection)?;

    loop {
        const PAYLOAD: &[u8] = b"Hello, world!";

        match connection.publish(PAYLOAD).await {
            Ok(delivery) => println!("publish successful. response: {delivery:?}"),
            Err(error) => println!("publish failed. error: {error}"),
        }
    }
}

/// Run the program, returning an [`Error`] on failure.
fn run() -> Result<(), Error> {
    // Set up the tokio runtime. There's a macro `tokio::main` for this, but I prefer doing it
    // manually.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .build()
        .map_err(Error::Runtime)?;

    // Start the async runtime with `run_async` as the entrypoint. Return value is passed up.
    runtime.block_on(run_async())
}

/// Entry point. Calls [`run`] and then checks the result, printing error information if present.
fn main() -> ExitCode {
    if let Err(e) = run() {
        print_error(&e);
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

/// Prints out an error's description and its sources.
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
    Connection(crate::amqp_connection::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Runtime(_) => f.write_str("failed to create async runtime"),
            Error::Connection(_) => f.write_str("failed to create AMQP connection"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Runtime(source) => Some(source),
            Error::Connection(source) => Some(source),
        }
    }
}
