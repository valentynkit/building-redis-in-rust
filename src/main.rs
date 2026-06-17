use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    codecrafters_redis::run()?;
    Ok(())
}
