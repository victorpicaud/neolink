use clap::Parser;

/// The healthcheck command tells whether a running neolink still works: it
/// exits with 0 if so, and 1 if part of it stopped answering
#[derive(Parser, Debug)]
pub struct Opt {
    /// How long a part may go without a sign of life, in seconds
    #[arg(long, default_value_t = 10)]
    pub timeout: u64,
    /// Only this camera, besides neolink itself
    pub camera: Option<String>,
}
