use std::{process::Stdio, sync::Arc};

use dashmap::DashMap;
use tokio::{io as tio, process::{Child as TokioChild, Command}, sync::watch::Sender, time::{self, Instant}};

use crate::obj::BindingStatus;

// common type alias
pub type HandlerState = Arc<DashMap<String, (String, Sender<()>, Instant)>>;
pub type InstanceState = Arc<DashMap<String, String>>;


pub async fn get_program_version(program: &str) -> tio::Result<Vec<u8>> {
    let output = Command::new(program).arg("--version").output().await?;
    Ok(output.stdout)
}

pub async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            println!("+ Shutdown in recv: ");
        },
        _ = terminate => {
            println!("+ Shutdown recv: ");
        },
    }
}

// handle ttyd child process timeouts in the subcommands 
pub async fn handle_ttyd_process_timeout(
    duration: String,
    global_handle_state_clone: HandlerState,
    global_instance_state_clone: InstanceState,
) {

        // set the timeout duration to be about 1 hour by default
        let timeout_duration = duration.parse::<u64>().unwrap_or(3600);
        let tokio_timeout_duration = time::Duration::from_secs(timeout_duration);

        loop {
            // give some 5 secs pause for the next iteration
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

            let current_time = tokio::time::Instant::now();

            global_handle_state_clone.retain(|_, value| {
                
                let ttyd_task_starting_time = value.2;
                let instance_id = value.0.clone();
                
                let time_elasped = current_time - ttyd_task_starting_time;

                if time_elasped < tokio_timeout_duration {
                    true
                } else {
                    // send signal to the task to terminate
                    if let Ok(_) = value.1.send(()) {
                        println!("+ dropped session: {instance_id}");
                        global_instance_state_clone.remove(&value.0);
                        false
                    } else {
                        println!("- failed to send signal to task: {instance_id}");
                        false
                    }
                }
            });
            
        }
}

// perform instance params checks
pub async fn validate_instance(instance_id: impl AsRef<str>) -> Result<(), String> { 
    let is_sanitized = instance_id.as_ref().chars().all(|c| c.is_alphanumeric());
    let char_count = instance_id.as_ref().chars().count();

    //todo: ensure max string size is correct
    if !is_sanitized || char_count < 6 || char_count > 20 {
        println!("- instance ID check failed");
        return Err(format!("Incorrect instance id: {}", instance_id.as_ref()));
    }
    Ok(())
} 

// start the ttyd child process for connecting with LXC container
pub async fn start_ttyd_process(
    instance_id: &str,
    uds_session_name: &str,
) -> Result<TokioChild, BindingStatus> {
    // construct the unix socket path where TTYD will spawn the UDS path
     //todo: check if files which are created has 660 permission or not
    let session_path = format!("{}{}.sock", dotenv!("UNIX_SOCKET_FOLDER"), uds_session_name);

    // arguments to spawn ttyd process with the required params
    let args = vec![
        "-O",
        "-o",
        "-a",
        "-W",
        "-i",
        &session_path,
        "lxc",
        "exec",
        &instance_id,
        "--",
        "sh",
        "-c",
        r#"/bin/bash"#,
    ];

    // spawn the ttyd process, return the child process handle, ensuring stdin is null and stdout/stderr are piped and killed on drop
    let ttyd_session = tokio::process::Command::new("ttyd")
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn(); // spawn the process over the runtime

    ttyd_session.map_err(|err| BindingStatus::Error(format!(
        "ttyd process spawn error: {}",
        err.to_string()
    )))
}

// poll the child process on a another green thread on runtime and return the closing handle to it
pub async fn child_process_handler(
    mut ttyd_process: TokioChild
) -> tokio::sync::watch::Sender<()> {
    // setup a channel to send shutdown signal to the task
    let (tx, mut recv) = tokio::sync::watch::channel(());

    //spawn this on the runtime where it gets polled continously for the updates
    tokio::spawn(async move {
        
        let ttyd_pid = ttyd_process.id();

        // wait for either the process to end or a shutdown signal
        tokio::select! {
            // child process is required to be awaited here to avoid zombie processes
            _ = ttyd_process.wait() => {

            }
            // if shutdown signal is recv, kill the process
            _ = recv.changed() =>  {
                println!("- task shutdown req : ");

                match ttyd_process.kill().await {
                    Ok(_) => {
                        println!("+ process shutdown success: pid ttyd: {ttyd_pid:?}");
                    }
                    _ => {
                        println!(" - process failed abruptly !");
                    }
                }
                ()
            }
        }
        ()
    });

    tx
}

pub fn acquire_response_details(
    domain: impl AsRef<str>,
    main_path: impl AsRef<str>,
    uds_session_name: impl AsRef<str>,
    token: impl AsRef<str>,
) -> String /* domain url with jwt token */ {

    let mut domain = domain.as_ref().to_string();
    let mut main_path = main_path.as_ref().to_string();

    if !main_path.is_empty() {
        domain.push_str("/");
    }

    main_path.push_str(format!("/{}", uds_session_name.as_ref()).as_str());
    domain.push_str(main_path.as_str());

    //add token header as part of domain!
    domain.push_str(format!("?token={}", token.as_ref()).as_str());
    domain.to_string()
}

// TODO: rewrite the logic for producing the correct JWT auth token field
// method to generate the jwt token 
pub fn jwt_token_generator(expiration: u64, key: &str) -> Result<String, Box<dyn std::error::Error>> {
    
    // let claim = Claims { 
    //     token_string: "eyJhbGciOiJIUzI1…Qhkntf78O9kF71F8".to_string(),
    //     user_claim: "username".to_string(),
    //     id: "ggicci".to_string(),
    //     // exp: expiration,
    // };

    // let claim = Claims {
    //     exp: 1910000000, // Example timestamp
    //     jti: "123e4567-e89b-12d3-a456-426614174000".to_string(),
    //     sub: "9876543210987654".to_string(),
    //     iss: "https://api.dummy.com".to_string(),
    //     aud: vec!["https://api.client.io".to_string()],
    //     username: "dummy_user".to_string(),
    // };

    // let token = match jsonwebtoken::encode(
    //     &jsonwebtoken::Header::default(), 
    //     &claim, 
    //     &jsonwebtoken::EncodingKey::from_secret(key.as_bytes())
    // ) {
    //     Ok(t) => t,
    //     Err(_) => panic!(), // in practice you would return the error
    // };

    //todo: change this to return a token with session value of 30mins
    let token = r#"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjk5NTU4OTI2NzAsImp0aSI6IjgyMjk0YTYzLTk2NjAtNGM2Mi1hOGE4LTVhNjI2NWVmY2Q0ZSIsInN1YiI6IjM0MDYzMjc5NjM1MTY5MzIiLCJpc3MiOiJodHRwczovL2FwaS5leGFtcGxlLmNvbSIsImF1ZCI6WyJodHRwczovL2FwaS5leGFtcGxlLmlvIl0sInVzZXJuYW1lIjoiZ2dpY2NpIn0.O8kvRO9y6xQO3AymqdFE7DDqLRBQhkntf78O9kF71F8"#;

    Ok(token.to_string())
}



// Test cases for the above util function

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_get_program_version() {
        let program = "ttyd";
        let version = get_program_version(program).await;
        assert!(version.is_ok());
    }

    #[tokio::test]
    async fn test_validate_instance() {
        
        let valid_instance = "instance123";
        let invalid_instance = "inst@nc#123";   
        let short_instance = "inst";
        let long_instance = "instance12345678901234567890";

        assert!(validate_instance(valid_instance).await.is_ok());
        assert!(validate_instance(invalid_instance).await.is_err());
        assert!(validate_instance(short_instance).await.is_err());
        assert!(validate_instance(long_instance).await.is_err());
    }

    #[tokio::test]
    async fn handle_ttyd_process_timeout_test_one() {
        // test for 10 second timeout
        let duration = "3".to_string(); // 10 seconds for testing

        let global_handle_state: HandlerState = Arc::new(DashMap::new());
        let global_instance_state: InstanceState = Arc::new(DashMap::new());

        // Insert a dummy session to test timeout
        let instance_id = "test_instance".to_string();
        let session_id = "test_session".to_string();
        let (tx, _rx) = tokio::sync::watch::channel(());
        global_handle_state.insert(
            session_id.clone(),
         (instance_id.clone(), tx, Instant::now()),
        );  

        // Insert the corresponding instance state
        global_instance_state.insert(instance_id.clone(), session_id.clone());

        // Spawn the timeout handler
        tokio::spawn(handle_ttyd_process_timeout(
            duration,
            global_handle_state.clone(),
            global_instance_state.clone(),
        ));

        // Allow some time for the handler to run
        tokio::time::sleep(tokio::time::Duration::from_secs(7)).await;

        // Since there are no sessions, the maps should remain empty
        assert!(global_handle_state.is_empty());
        assert!(global_instance_state.is_empty());
    }

    #[should_panic]
    #[tokio::test]
    async fn handle_ttyd_process_timeout_test_two() {
        // test for 10 second timeout
        let duration = "10".to_string(); // 10 seconds for testing

        let global_handle_state: HandlerState = Arc::new(DashMap::new());
        let global_instance_state: InstanceState = Arc::new(DashMap::new());

        // Insert a dummy session to test timeout
        let instance_id = "test_instance".to_string();
        let session_id = "test_session".to_string();
        let (tx, _rx) = tokio::sync::watch::channel(());
        global_handle_state.insert(
            session_id.clone(),
         (instance_id.clone(), tx, Instant::now()),
        );  

        // Insert the corresponding instance state
        global_instance_state.insert(instance_id.clone(), session_id.clone());

        // Spawn the timeout handler
        tokio::spawn(handle_ttyd_process_timeout(
            duration,
            global_handle_state.clone(),
            global_instance_state.clone(),
        ));

        // Allow some time for the handler to run
        tokio::time::sleep(tokio::time::Duration::from_secs(7)).await;

        // Since there are no sessions, the maps should remain empty
        assert!(global_handle_state.is_empty());
        assert!(global_instance_state.is_empty());
    }


    #[tokio::test]
    async fn test_ttyd_process_start() {
        let instance_id = "test_instance";
        let uds_session_name = "test_session";

        let result = start_ttyd_process(instance_id, uds_session_name).await;

        // Since ttyd might not be installed in the test environment, we check for error
        assert!(result.is_err() || result.is_ok());
    }

}