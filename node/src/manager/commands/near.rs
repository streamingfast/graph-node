use graph::prelude::Error;
use graph_chain_near::NearIndexer;

pub fn run(homedir: String) -> Result<(), Error> {
    let idx = NearIndexer::new(homedir.into());
    idx.run();
    Ok(())
}

pub fn init(homedir: String) -> Result<(), Error> {
    let idx = NearIndexer::new(homedir.into());
    idx.init();
    Ok(())
}
