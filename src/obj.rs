use axum::{
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use http_error_derive::HttpError;
use serde::{Deserialize, Serialize};

// Struct for the response of container binding
#[derive(Serialize, Deserialize, Debug)]
pub struct ContainerBindingResponse {
    pub terminal_session_name: String,
    pub access_token: String,
    pub url: String,
    pub status: BindingStatus,
}

// Basic Error enum for handling the error responses from the handlers
#[derive(Serialize, Deserialize, Debug, HttpError)]
pub enum BindingStatus {
    Binding,
    #[http(code = 400, message = "Request failed")]
    Failed(String),
    Live,
    #[http(code = 500, message = "Internal error")]
    Error(String),
    #[http(code = 500, message = "Error reading process value")]
    ProcessReadError(String),
    #[http(code = 409, message = "Session already attached for this instance")]
    SessionRunning(String),
    #[http(code = 404, message = "Unknown session")]
    NotFound(String),
    #[http(code = 429, message = "At capacity: too many active sessions")]
    AtCapacity,
    #[http(code = 500, message = "File creation error by ttyd")]
    FileCreationError(String),
}

impl IntoResponse for BindingStatus {
    fn into_response(self) -> Response<Body> {
        // Honor the status code annotated on each variant (via the HttpError
        // derive's http_code()) instead of collapsing everything to 500.
        let status = self
            .http_code()
            .and_then(|code| StatusCode::from_u16(code).ok())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

        // Prefer the dynamic detail a variant carries; fall back to its static
        // annotated message for the unit variants.
        let message = match &self {
            BindingStatus::Failed(m)
            | BindingStatus::Error(m)
            | BindingStatus::ProcessReadError(m)
            | BindingStatus::SessionRunning(m)
            | BindingStatus::NotFound(m)
            | BindingStatus::FileCreationError(m) => m.clone(),
            _ => self.http_message().unwrap_or("Some Error").to_string(),
        };

        (status, message).into_response()
    }
}

// Implement `IntoResponse`
impl IntoResponse for ContainerBindingResponse {
    fn into_response(self) -> Response {
        match serde_json::to_string(&self) {
            Ok(json_body) => {
                let mut headers = axum::http::HeaderMap::new();
                headers.insert(
                    "Content-Type",
                    axum::http::HeaderValue::from_static("application/json"),
                );

                (StatusCode::OK, headers, json_body).into_response()
            }
            Err(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to serialize response",
            )
                .into_response(),
        }
    }
}


//TODO: work on supporting the claims fields

// #[derive(Debug, Serialize, Deserialize)]
// pub struct Claims {
//     pub aud: String,
//     pub uid: String,
//     pub user_id: String,
//     pub username: String, 
//     pub login: String, 
//     pub exp: u64,
// }

// #[derive(Debug, Serialize, Deserialize)]
// pub struct Claims {
//     pub exp: u64,
//     pub jti: String,
//     pub sub: String,
//     pub iss: String,
//     pub aud: Vec<String>,
//     pub username: String,
// }