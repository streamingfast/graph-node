use graph::prelude::Error;

pub fn run(homedir: String) -> Result<(), Error> {
    println!("NEAR homedir: {}", homedir);
    Ok(())
}
