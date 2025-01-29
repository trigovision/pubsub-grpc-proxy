use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone)]
pub enum AuthMethod {
    ApplicationDefaultCredentials(String),
    ServiceAccountKey(String, PathBuf),
}

#[derive(Clone)]
pub(crate) struct TokenInterceptor {
    project_id: String,
    token: Arc<gouth::Token>,
}

impl TokenInterceptor {
    pub fn with_application_default_credentials(
        project_id: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let token = gouth::Builder::new()
            .scopes(&[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/pubsub",
            ])
            .build()?;

        Ok(Self {
            project_id: project_id.to_string(),
            token: Arc::new(token),
        })
    }

    pub fn with_service_account_key<P: AsRef<Path>>(
        project_id: &str,
        key_path: P,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let token = gouth::Builder::new().file(key_path.as_ref()).build()?;

        Ok(Self {
            project_id: project_id.to_owned(),
            token: Arc::new(token),
        })
    }

    #[allow(dead_code)]
    pub fn project_id(&self) -> &str {
        &self.project_id
    }
}

impl TryFrom<AuthMethod> for TokenInterceptor {
    type Error = Box<dyn std::error::Error>;

    fn try_from(auth_method: AuthMethod) -> Result<Self, Self::Error> {
        match auth_method {
            AuthMethod::ApplicationDefaultCredentials(project_id) => {
                Self::with_application_default_credentials(&project_id)
            }
            AuthMethod::ServiceAccountKey(project_id, key_path) => {
                Self::with_service_account_key(&project_id, key_path)
            }
        }
    }
}

impl tonic::service::Interceptor for TokenInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        let meta = self
            .token
            .header_value()
            .map_err(|err| tonic::Status::unavailable(err.to_string()))?
            .parse()
            .expect("we provide the tokens, and they are valid ascii");
        request.metadata_mut().insert("authorization", meta);
        Ok(request)
    }
}
