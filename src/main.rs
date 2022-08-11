use std::collections::HashMap;
use std::env;

mod error;
mod io;
mod transaction;

pub use crate::error::*;
pub use crate::io::*;
pub use crate::transaction::*;

fn main() -> Result<(), Error> {
    // cli
    let args: Vec<String> = env::args().collect();
    let filepath = args.get(1).expect("Filepath expected");

    // Input from csv
    let buf = open_file(filepath)?;
    let txs = read_csv(buf)?;

    // State
    let mut accounts: HashMap<u16, ClientAccount> = HashMap::new();
    let mut tx_states: HashMap<u32, TxState> = HashMap::new();

    // Process transactions
    for tx in txs.clone() {
        let _result = process_tx(tx, &mut accounts, &mut tx_states);
    }

    // Output to Stdout
    output_to_stdout(accounts, &mut std::io::stdout())?;
    Ok(())
}
