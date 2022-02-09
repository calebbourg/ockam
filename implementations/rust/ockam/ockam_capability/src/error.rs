use ockam_core::Error;

/// Represents the failures that can occur in
/// an Ockam Capability operation
#[derive(Clone, Copy, Debug)]
pub enum CapabilityError {
    /// ToDo
    ToDo = 1,
}

impl CapabilityError {
    /// Integer code associated with the error domain.
    pub const DOMAIN_CODE: u32 = 21_000;
    /// Descriptive name for the error domain.
    pub const DOMAIN_NAME: &'static str = "OCKAM_CAPABILITY";
}

impl From<CapabilityError> for Error {
    fn from(err: CapabilityError) -> Self {
        Self::new(
            CapabilityError::DOMAIN_CODE + (err as u32),
            CapabilityError::DOMAIN_NAME,
        )
    }
}
