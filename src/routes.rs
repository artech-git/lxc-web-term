
use crate::{obj::{BindingStatus, ContainerBindingResponse}, utils::{child_process_handler, HandlerState, InstanceState}};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use dashmap::mapref::entry::Entry;

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

    // Atomically check-and-reserve the instance slot via the DashMap entry API.
    // The previous code did `get` then later `insert` as two separate steps, so
    // two concurrent requests for the same instance could both pass the check and
    // both spawn a ttyd process. Taking the entry holds the shard lock across the
    // check and the reservation, so a concurrent request observes the slot as
    // occupied and is rejected. The reservation is rolled back below if the spawn
    // itself fails, so a failed start does not wedge the instance.
    let uds_session_name = match session_handle.entry(instance_id.clone()) {
        Entry::Occupied(existing) => {
            println!("+ session already found");
            return Err(BindingStatus::SessionRunning(existing.get().clone()));
        }
        Entry::Vacant(slot) => {
            let uds = uuid::Uuid::new_v4().as_hyphenated().to_string();
            slot.insert(uds.clone());
            uds
        }
    };

    let ttyd_session = crate::utils::start_ttyd_process(&instance_id, &uds_session_name).await;

    match ttyd_session {
        Ok(ttyd_process) => {
            println!("+ listening on domain socket: {uds_session_name}");

            let shutdown_signal = child_process_handler(ttyd_process).await;

            // the session_handle slot was already reserved above via the entry
            // API; only the job_handle entry remains to be recorded here
            job_handle.insert(uds_session_name.clone(), (instance_id.clone(), shutdown_signal, tokio::time::Instant::now()));

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
            // roll back the reservation so a failed spawn doesn't leave the
            // instance permanently marked as attached
            session_handle.remove(&instance_id);
            println!("- ttyd process failed to start: {:?}", binding_err);
            return Err(binding_err);
        }
    }
    unreachable!("unreachable code section")
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
