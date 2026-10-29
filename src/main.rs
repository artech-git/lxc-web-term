#[macro_use]
extern crate dotenv_codegen;

use core::time;
use std::sync::Arc;

use axum::{routing::get, Router};
use dashmap::DashMap;
use tokio::{sync::watch::Sender, time::Instant};
use utils::shutdown_signal;

use crate::utils::{HandlerState, InstanceState};

mod obj;
mod routes;
mod utils;

// runtime variable settings

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    let port = format!(
        "{}:{}",
        dotenv!("SERVER_INTERFACE_ADDR"),
        dotenv!("TERM_SERVER_PORT")
    );
    println!("+ Server runnning on address: {port}");

    // Insert checks for presence for bore & ttyd !
    let ttyd_util = "ttyd";

    let ttyd_version = b"ttyd version 1.7.4-68521f5";

    let ttyd_check = utils::get_program_version(&ttyd_util).await;

    match ttyd_check {
        Ok(ttyd) if ttyd == ttyd_version => {
            println!("✔ ttyd present ");
        }
        Err(ttyd_err) => {
            println!("- ttyd program failed to run !: {ttyd_err}");
            // return; //exit program gracefully
        }
        _ => {
            println!("- ttyd version mismatch, program failed abruptly !");
            // return;
        }
    };

    let listener = match tokio::net::TcpListener::bind(port).await {
        Ok(val) => {
            println!("+ port binding succeded");
            val
        }
        Err(err) => {
            println!("- port binding failed: {err}");
            return;
        }
    };

    // maps instances id  with session id
    let global_instance_state: InstanceState = Arc::new(DashMap::new());

    // maps session_id with instance task handle
    // key => session_id, value => (instance_id, Sender, starting_time)
    let global_handle_state: HandlerState = Arc::new(DashMap::new());

    //clone rest of the handlers for passing to handlers
    let duration = dotenv!("TTYD_SESSION_TIMEOUT").to_string();
    let global_instance_state_clone = global_instance_state.clone();
    let global_handle_state_clone = global_handle_state.clone();

    // spawn a task to handle ttyd process timeouts
    tokio::spawn(utils::handle_ttyd_process_timeout(
            duration,
            global_handle_state_clone,
            global_instance_state_clone,
        )
    );

    let route_1 = Router::new()
        .route(
            "/start_web_session/{instance_uid}",
            get(routes::open_terminal),
        )
        .with_state((global_handle_state.clone(), global_instance_state.clone()));
    let route_2 = Router::new()
        .route("/terminate_web_session/{uid}", get(routes::close_terminal))
        .with_state((global_handle_state.clone(), global_instance_state.clone()));

    // heartbeat endpoint to extend an active session's idle deadline
    let route_3 = Router::new()
        .route("/extend_web_session/{session_id}", get(routes::extend_terminal))
        .with_state((global_handle_state.clone(), global_instance_state.clone()));

    let app = Router::new().merge(route_1).merge(route_2).merge(route_3);

    let server = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal());

    // poll the server future, until the error occurs / server is stopped
    if let Err(err) = server.await {
        println!("- error starting the application {err}");
    }
}
