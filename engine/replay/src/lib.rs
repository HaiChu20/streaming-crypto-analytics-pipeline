pub mod features;
pub mod input;
pub mod output;
pub mod row;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
pub use engine_core::{ENGINE_VERSION, SECOND};
pub fn error(message: impl Into<String>) -> Error {
    std::io::Error::other(message.into()).into()
}
