
use crate::{obj::{BindingStatus, ContainerBindingResponse}, utils::{child_process_handler, HandlerState, InstanceState}};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};

/*
    Route for accepting the terminal based upon instance_id which we get from the user endpoint
    State carries the global handlerState and instanceState hashmap references
*/

//parametrize the function to accept diff. confg. as per future requirement
pub async fn open_terminal(
    Path(instance_id): Path<String>,
    State((job_handle, session_handle)): State<(
        HandlerState,
        InstanceState,
    )>,
) -> impl IntoResponse {
    // apply checks for presence of container on system !

    // perform checks against the endpoint instance_id input
    if let Err(validate_err) = crate::utils::validate_instance(&instance_id).await {
        println!("- instance validation failed: {validate_err}");
        return Err(BindingStatus::Failed(validate_err));
    }

    //check if session already exists for a given instance id
    if let Some(session_id) = session_handle.get(&instance_id) {
        println!("+ session already found");
        return Err(BindingStatus::SessionRunning(session_id.clone()));
    }
   
    let uds_session_name = uuid::Uuid::new_v4().as_hyphenated().to_string();

    let ttyd_session = crate::utils::start_ttyd_process(&instance_id, &uds_session_name).await;

    match ttyd_session {
        Ok(ttyd_process) => {
            println!("+ listening on domain socket: {uds_session_name}");

            let shutdown_signal = child_process_handler(ttyd_process).await;

            // update hashmaps for proper information
            job_handle.insert(uds_session_name.clone(), (instance_id.clone(), shutdown_signal, tokio::time::Instant::now()));
            session_handle.insert(instance_id.clone(), uds_session_name.clone());

            let jwt_secret = dotenv!("MZ_TERM_JWT_SINGING_SECRET"); 
            let jwt_timeout = dotenv!("MZ_TERM_JWT_TIMEOUT").parse::<u64>().unwrap_or(3600);
            
            match crate::utils::jwt_token_generator(jwt_timeout, jwt_secret) {
                Ok(token) => {

                    let domain = dotenv!("SERVING_NAME").to_string();
                    let main_path = dotenv!("MACHINE_ID").to_string();
                    
                    // generate the domain over which ttyd web terminal can be accessed
                    let domain = crate::utils::acquire_response_details(
                        domain,
                        main_path,
                        uds_session_name.clone(),
                        token.clone(),
                    );

                    let container_binding_resp = ContainerBindingResponse {
                        terminal_session_name: uds_session_name,
                        access_token: token,
                        url: domain,
                        status: BindingStatus::Live,
                    };

                    return Ok(container_binding_resp);
                }
                Err(_) => {
                    return Err(BindingStatus::Error("JWT token generation failed".to_string()));
                }
            }

        }
        Err(binding_err) => {
            println!("- ttyd process failed to start: {:?}", binding_err);
            return Err(binding_err);
        }
    }
    unreachable!("unreachable code section")
}


/*
    Route for extending (resetting the idle deadline of) a live terminal session.

    The idle-timeout sweep measures inactivity from the last-activity Instant
    stored for each session. A client keeps an actively used shell alive by
    calling this endpoint as a heartbeat; each call resets that Instant to now,
    so only genuinely idle sessions are reaped.
*/
pub async fn extend_terminal(
    Path(session_id): Path<String>,
    State((job_handle, _session_handle)): State<(
        HandlerState,
        InstanceState,
    )>,
) -> impl IntoResponse {
    // session id is the uds session name, i.e. the key of the job handle map
    match job_handle.get_mut(&session_id) {
        Some(mut entry) => {
            // refresh the last-activity timestamp, pushing the idle deadline out
            entry.2 = tokio::time::Instant::now();
            println!("+ extended session: {session_id}");
            (
                StatusCode::OK,
                format!("+ session extended: {session_id}"),
            )
        }
        None => {
            println!("- extend failed, unknown session: {session_id}");
            (
                StatusCode::NOT_FOUND,
                format!("- unknown session: {session_id}"),
            )
        }
    }
}


/*
    Route for closing the terminal session based upon session_id which we get from the user endpoint
    State carries the global handlerState and instanceState hashmap references

    acquire the respective shutdown channel signal sender based upon the session_id if present
*/

// close terminal session handler
pub async fn close_terminal(
    Path(session_id): Path<String>,
    State((job_handle, session_handle)): State<(
        HandlerState,
        InstanceState,
    )>,
) -> impl IntoResponse {

    //session id is basically the uds session name
    // 1. If session id is present in the job_handle hashmap, acquire the shutdown sender channel and send the shutdown signal
    let resp = match job_handle.contains_key(&session_id) {
        true => {
            // 2. attempt to remove the job from the hashmap
            if let Some((sess, handle)) = job_handle.remove(&session_id) {
                
                // 1.2.1 send the shutdown signal to the task handler
                if let Ok(_) = handle.1.send(()) {
                    // cancel this running task
                    println!("+ dropped session: {sess}");
                    let instance_id = handle.0;
                    // once the task is dropped, remove the session from session_handle hashmap
                    session_handle.remove(&instance_id);
                    Response::new("+ successfully closed the terminal session".to_string())
                
                // 1.2.2 if sending shutdown signal fails, 
                // we preserve the session details and let dashmap handle the cleanup on timeout
                } else {
                    //session cancellation failed
                    println!("- failed to cancel the session: {sess}");
                    Response::new("- failed to cancel the session".to_string())
                }

            // 3 .if removal from hashmap fails, due to lock or contention
            } else {
                println!("- failed to close session id");
                Response::new("- job not found for session_key or already removed".to_string())
            }
        }

        // session id not found in the hashmap, hence already closed or invalid session id
        false => {
            println!("- failed to find the task for a given session key");
            let mut resp = Response::new(format!("- task not found for session key"));
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            resp
        }
    };

    return resp;
}
