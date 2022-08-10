use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fmt;
use std::fs;
use std::io::prelude::*;
use std::io::BufReader;

#[derive(Debug, Clone)]
pub struct Error {
    pub message: String,
}
impl std::error::Error for Error {}
impl Error {
    pub fn new(message: &str) -> Error {
        Error {
            message: message.to_string(),
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl From<csv::Error> for Error {
    fn from(err: csv::Error) -> Self {
        Self {
            message: format!("CSV Error: {}", err.to_string()),
        }
    }
}
impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self {
            message: format!("IO Error: {}", err.to_string()),
        }
    }
}

fn read_lines(path: &str) {
    let file = fs::File::open(path).unwrap();
    let buf_read = BufReader::new(file);

    let mut lines = buf_read.lines();

    while let Some(Ok(line)) = lines.next() {
        println!("{}", line);
    }
}

fn open_file(path: &str) -> Result<BufReader<fs::File>, Error> {
    let file = fs::File::open(path).expect(&format!("Unable to open file: {}", path));
    let buf_reader = BufReader::new(file);
    Ok(buf_reader)
}

fn read_csv<R: std::io::Read>(buf: R) -> Result<Vec<Tx>, Error> {
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .trim(csv::Trim::All)
        .from_reader(buf);

    let mut data: Vec<Tx> = vec![];
    for result in csv_reader.deserialize() {
        let tx: Tx = result?;
        println!("Tx: {:?}", tx);
        data.push(tx);
    }

    Ok(data)
}

// Steps:
// 1. Read file: process line by line
// 2. create in memory accounts for new users
// 3. create transaction log for each user in the form of a vector containing all transactions
// 4. read the in memory logs, calculate client accounts balance
// 5. output to stdout
//

// Other considerations:
// - how would you go about having multiple workers accessing the same log?
//   ie having concurrent writes to the log
//  - Idea: have a client account specific mutex to lock modifications to a specific account
//  A worker would pickup a transaction line and attempt to execute it, if the row is locked,
//   it would either await for it to become available or put it back into the queue (front)
// - Alternative: adtop a client based partition strategy
// - Another idea: implement it as a future that doesn't resolve until it can

#[derive(Debug, Deserialize, PartialEq, Clone)]
struct Tx {
    #[serde(rename = "type")]
    pub type_: TxType,
    #[serde(rename = "client")]
    pub client_id: u16,
    #[serde(rename = "tx")]
    pub tx_id: u32,
    pub amount: Option<f32>,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "snake_case")]
enum TxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, PartialEq)]
struct TxState {
    pub amount: f32,
    pub client_id: u16,
    pub disputed: bool,
    // pub resolved: bool,
    pub charged_back: bool,
}

impl TxState {
    fn new(amount: f32, client_id: u16) -> Self {
        Self {
            amount,
            client_id,
            disputed: false,
            charged_back: false,
        }
    }
}

fn round_serialize<S>(x: &f32, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let x = (x * 10000.0).round() / 10000.0;
    s.serialize_f32(x)
}

#[derive(Debug, Serialize, PartialEq)]
struct ClientAccount {
    client: u16,
    #[serde(serialize_with = "round_serialize")]
    pub available: f32,
    #[serde(serialize_with = "round_serialize")]
    pub held: f32,
    #[serde(serialize_with = "round_serialize")]
    pub total: f32,
    pub locked: bool,
}

impl ClientAccount {
    fn new(client_id: u16) -> Self {
        Self {
            client: client_id,
            available: 0.0,
            held: 0.0,
            total: 0.0,
            locked: false,
        }
    }
}

fn output_to_stdout(
    accounts: HashMap<u16, ClientAccount>,
    output: &mut impl Write,
) -> Result<(), Error> {
    let mut writer = csv::WriterBuilder::new()
        .delimiter(b',')
        .has_headers(true)
        .from_writer(output);

    for account in accounts.into_values() {
        writer.serialize(account)?;
    }
    writer.flush()?;
    Ok(())
}

fn process_tx(
    tx: Tx,
    accounts: &mut HashMap<u16, ClientAccount>,
    tx_states: &mut HashMap<u32, TxState>,
) -> Result<(), Error> {
    let client_id = tx.client_id;
    let tx_id = tx.tx_id;
    println!("\n\nclient: {}, tx: {}", client_id, tx_id);
    let mut account = accounts
        .entry(client_id)
        .or_insert(ClientAccount::new(client_id));
    println!("================================================");
    println!("BEFORE: client: {}, account {:?}", client_id, account);
    println!(">>>>>>>>>>>>>");
    println!("Tx: {:?}", tx);
    println!("              <<<<<<<<<<<<<<<");

    if account.locked == true {
        return Ok(());
    }

    match tx_states.get_mut(&tx_id) {
        Some(tx_state) => match tx.type_ {
            TxType::Deposit => {}
            TxType::Withdrawal => {}
            TxType::Dispute => {
                if tx_state.disputed == false {
                    tx_state.disputed = true;
                    tx_state.charged_back = false;
                    let amount = tx_state.amount.abs();
                    account.available -= amount;
                    account.held += amount;
                }
            }
            TxType::Resolve => {
                if tx_state.disputed == true {
                    tx_state.disputed = false;
                    tx_state.charged_back = false;
                    let amount = tx_state.amount.abs();
                    account.available += amount;
                    account.held -= amount;
                };
            }
            TxType::Chargeback => {
                if tx_state.disputed == true {
                    tx_state.disputed = false;
                    tx_state.charged_back = true;
                    let amount = tx_state.amount;
                    account.total -= amount;
                    account.held -= amount;
                    account.locked = true;
                }
            }
        },
        None => match tx.type_ {
            TxType::Deposit => {
                let amount = tx
                    .amount
                    .ok_or(Error::new("Deposit transaction expected to have an amount"))?;
                tx_states.insert(tx_id, TxState::new(amount, tx.client_id));
                account.total += amount.abs();
                account.available += amount.abs();
            }
            TxType::Withdrawal => {
                let amount = tx.amount.ok_or(Error::new(
                    "Withdrawal transaction expected to have an amount",
                ))?;
                if amount < account.available {
                    tx_states.insert(tx_id, TxState::new(-amount, tx.client_id));
                    account.total -= amount;
                    account.available -= amount;
                }
            }
            TxType::Dispute => {}
            TxType::Resolve => {}
            TxType::Chargeback => {}
        },
    };
    println!("AFTER client: {}, account {:?}", client_id, account);
    println!("================================================");
    Ok(())
}

fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    let filepath = args.get(1).expect("Filepath expected");
    // println!("{:?}", args);
    // println!("Filename: {}", filepath);
    // read_lines(filepath);

    let buf = open_file(filepath)?;
    let txs = read_csv(buf)?;

    // State
    let mut accounts: HashMap<u16, ClientAccount> = HashMap::new();
    let mut tx_states: HashMap<u32, TxState> = HashMap::new();

    // Process transactions
    for tx in txs.clone() {
        process_tx(tx, &mut accounts, &mut tx_states)?;
    }
    println!("tx_states: {:#?} \n\n", tx_states);
    println!("accounts: {:#?} \n\n", accounts);

    // Output to Stdout
    output_to_stdout(accounts, &mut std::io::stdout())?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn read_csv_from_buffer() {
        let data = "\
type, client, tx, amount
deposit, 1, 1, 1.0
withdrawal, 2, 5, 3.0
dispute, 1, 1,
resolve, 1, 1,
chargeback, 1, 1,
";
        assert_eq!(
            read_csv(data.as_bytes()).unwrap(),
            vec![
                Tx {
                    type_: TxType::Deposit,
                    client_id: 1,
                    tx_id: 1,
                    amount: Some(1.0),
                },
                Tx {
                    type_: TxType::Withdrawal,
                    client_id: 2,
                    tx_id: 5,
                    amount: Some(3.0),
                },
                Tx {
                    type_: TxType::Dispute,
                    client_id: 1,
                    tx_id: 1,
                    amount: None,
                },
                Tx {
                    type_: TxType::Resolve,
                    client_id: 1,
                    tx_id: 1,
                    amount: None,
                },
                Tx {
                    type_: TxType::Chargeback,
                    client_id: 1,
                    tx_id: 1,
                    amount: None,
                }
            ]
        );
    }

    #[test]
    fn deposit() -> Result<(), Error> {
        let mut accounts: HashMap<u16, ClientAccount> = HashMap::new();
        let mut tx_states: HashMap<u32, TxState> = HashMap::new();
        let tx = Tx {
            type_: TxType::Deposit,
            client_id: 1,
            tx_id: 1,
            amount: Some(1.0),
        };
        process_tx(tx, &mut accounts, &mut tx_states)?;

        let account = accounts.get(&1).unwrap();
        assert_eq!(
            *account,
            ClientAccount {
                client: 1,
                available: 1.0,
                held: 0.0,
                total: 1.0,
                locked: false,
            }
        );
        Ok(())
    }

    #[test]
    fn dispute_deposit() -> Result<(), Error> {
        let mut accounts: HashMap<u16, ClientAccount> = HashMap::new();
        let mut tx_states: HashMap<u32, TxState> = HashMap::new();
        let txs = vec![
            Tx {
                type_: TxType::Deposit,
                client_id: 1,
                tx_id: 1,
                amount: Some(1.0),
            },
            Tx {
                type_: TxType::Dispute,
                client_id: 1,
                tx_id: 1,
                amount: None,
            },
        ];
        for tx in txs {
            process_tx(tx, &mut accounts, &mut tx_states)?;
        }

        let account = accounts.get(&1).unwrap();
        assert_eq!(
            *account,
            ClientAccount {
                client: 1,
                available: 0.0,
                held: 1.0,
                total: 1.0,
                locked: false,
            }
        );
        Ok(())
    }

    #[test]
    fn resolve_dispute() -> Result<(), Error> {
        let mut accounts: HashMap<u16, ClientAccount> = HashMap::new();
        let mut tx_states: HashMap<u32, TxState> = HashMap::new();
        let txs = vec![
            Tx {
                type_: TxType::Deposit,
                client_id: 1,
                tx_id: 1,
                amount: Some(1.0),
            },
            Tx {
                type_: TxType::Dispute,
                client_id: 1,
                tx_id: 1,
                amount: None,
            },
            Tx {
                type_: TxType::Resolve,
                client_id: 1,
                tx_id: 1,
                amount: None,
            },
        ];
        for tx in txs {
            process_tx(tx, &mut accounts, &mut tx_states)?;
        }

        let account = accounts.get(&1).unwrap();
        assert_eq!(
            *account,
            ClientAccount {
                client: 1,
                available: 1.0,
                held: 0.0,
                total: 1.0,
                locked: false,
            }
        );
        Ok(())
    }

    #[test]
    fn chargeback_dispute() -> Result<(), Error> {
        let mut accounts: HashMap<u16, ClientAccount> = HashMap::new();
        let mut tx_states: HashMap<u32, TxState> = HashMap::new();
        let txs = vec![
            Tx {
                type_: TxType::Deposit,
                client_id: 1,
                tx_id: 1,
                amount: Some(1.0),
            },
            Tx {
                type_: TxType::Dispute,
                client_id: 1,
                tx_id: 1,
                amount: None,
            },
            Tx {
                type_: TxType::Chargeback,
                client_id: 1,
                tx_id: 1,
                amount: None,
            },
        ];
        for tx in txs {
            process_tx(tx, &mut accounts, &mut tx_states)?;
        }

        let account = accounts.get(&1).unwrap();
        assert_eq!(
            *account,
            ClientAccount {
                client: 1,
                available: 0.0,
                held: 0.0,
                total: 0.0,
                locked: true,
            }
        );
        Ok(())
    }

    #[test]
    fn withdrawal() -> Result<(), Error> {
        let mut accounts: HashMap<u16, ClientAccount> = HashMap::new();
        let mut tx_states: HashMap<u32, TxState> = HashMap::new();
        let txs = vec![
            Tx {
                type_: TxType::Deposit,
                client_id: 1,
                tx_id: 1,
                amount: Some(10.0),
            },
            Tx {
                type_: TxType::Withdrawal,
                client_id: 1,
                tx_id: 2,
                amount: Some(7.0),
            },
        ];
        for tx in txs {
            process_tx(tx, &mut accounts, &mut tx_states)?;
        }

        let account = accounts.get(&1).unwrap();
        assert_eq!(
            *account,
            ClientAccount {
                client: 1,
                available: 3.0,
                held: 0.0,
                total: 3.0,
                locked: false,
            }
        );
        Ok(())
    }

    #[test]
    fn block_withdrawal() -> Result<(), Error> {
        let mut accounts: HashMap<u16, ClientAccount> = HashMap::new();
        let mut tx_states: HashMap<u32, TxState> = HashMap::new();
        let txs = vec![
            Tx {
                type_: TxType::Deposit,
                client_id: 1,
                tx_id: 1,
                amount: Some(5.0),
            },
            Tx {
                type_: TxType::Withdrawal,
                client_id: 1,
                tx_id: 2,
                amount: Some(7.0),
            },
        ];
        for tx in txs {
            process_tx(tx, &mut accounts, &mut tx_states)?;
        }

        let account = accounts.get(&1).unwrap();
        assert_eq!(
            *account,
            ClientAccount {
                client: 1,
                available: 5.0,
                held: 0.0,
                total: 5.0,
                locked: false,
            }
        );
        Ok(())
    }

    #[test]
    fn dispute_on_nonexistent_tx_is_ignored() -> Result<(), Error> {
        let mut accounts: HashMap<u16, ClientAccount> = HashMap::new();
        let mut tx_states: HashMap<u32, TxState> = HashMap::new();
        let txs = vec![
            Tx {
                type_: TxType::Deposit,
                client_id: 1,
                tx_id: 1,
                amount: Some(5.0),
            },
            Tx {
                type_: TxType::Dispute,
                client_id: 1,
                tx_id: 2,
                amount: None,
            },
        ];
        for tx in txs {
            process_tx(tx, &mut accounts, &mut tx_states)?;
        }

        let account = accounts.get(&1).unwrap();
        assert_eq!(
            *account,
            ClientAccount {
                client: 1,
                available: 5.0,
                held: 0.0,
                total: 5.0,
                locked: false,
            }
        );
        Ok(())
    }

    #[test]
    fn resolve_on_nondisputed_tx_is_ignored() -> Result<(), Error> {
        let mut accounts: HashMap<u16, ClientAccount> = HashMap::new();
        let mut tx_states: HashMap<u32, TxState> = HashMap::new();
        let txs = vec![
            Tx {
                type_: TxType::Deposit,
                client_id: 1,
                tx_id: 1,
                amount: Some(5.0),
            },
            Tx {
                type_: TxType::Resolve,
                client_id: 1,
                tx_id: 1,
                amount: None,
            },
        ];
        for tx in txs {
            process_tx(tx, &mut accounts, &mut tx_states)?;
        }

        let account = accounts.get(&1).unwrap();
        assert_eq!(
            *account,
            ClientAccount {
                client: 1,
                available: 5.0,
                held: 0.0,
                total: 5.0,
                locked: false,
            }
        );
        Ok(())
    }

    #[test]
    fn chargeback_on_nondisputed_tx_is_ignored() -> Result<(), Error> {
        let mut accounts: HashMap<u16, ClientAccount> = HashMap::new();
        let mut tx_states: HashMap<u32, TxState> = HashMap::new();
        let txs = vec![
            Tx {
                type_: TxType::Deposit,
                client_id: 1,
                tx_id: 1,
                amount: Some(5.0),
            },
            Tx {
                type_: TxType::Chargeback,
                client_id: 1,
                tx_id: 1,
                amount: None,
            },
        ];
        for tx in txs {
            process_tx(tx, &mut accounts, &mut tx_states)?;
        }

        let account = accounts.get(&1).unwrap();
        assert_eq!(
            *account,
            ClientAccount {
                client: 1,
                available: 5.0,
                held: 0.0,
                total: 5.0,
                locked: false,
            }
        );
        Ok(())
    }

    #[test]
    fn dispute_on_disputed_tx_is_ignored() -> Result<(), Error> {
        let mut accounts: HashMap<u16, ClientAccount> = HashMap::new();
        let mut tx_states: HashMap<u32, TxState> = HashMap::new();
        let txs = vec![
            Tx {
                type_: TxType::Deposit,
                client_id: 1,
                tx_id: 1,
                amount: Some(5.0),
            },
            Tx {
                type_: TxType::Dispute,
                client_id: 1,
                tx_id: 1,
                amount: None,
            },
            Tx {
                type_: TxType::Dispute,
                client_id: 1,
                tx_id: 1,
                amount: None,
            },
        ];
        for tx in txs {
            process_tx(tx, &mut accounts, &mut tx_states)?;
        }

        let account = accounts.get(&1).unwrap();
        assert_eq!(
            *account,
            ClientAccount {
                client: 1,
                available: 0.0,
                held: 5.0,
                total: 5.0,
                locked: false,
            }
        );
        Ok(())
    }

    #[test]
    fn block_tx_on_frozen_account() -> Result<(), Error> {
        let mut accounts: HashMap<u16, ClientAccount> = HashMap::new();
        let mut tx_states: HashMap<u32, TxState> = HashMap::new();
        let txs = vec![
            Tx {
                type_: TxType::Deposit,
                client_id: 1,
                tx_id: 1,
                amount: Some(5.0),
            },
            Tx {
                type_: TxType::Dispute,
                client_id: 1,
                tx_id: 1,
                amount: None,
            },
            Tx {
                type_: TxType::Chargeback,
                client_id: 1,
                tx_id: 1,
                amount: None,
            },
            Tx {
                type_: TxType::Deposit,
                client_id: 1,
                tx_id: 2,
                amount: Some(100.0),
            },
        ];
        for tx in txs {
            process_tx(tx, &mut accounts, &mut tx_states)?;
        }

        let account = accounts.get(&1).unwrap();
        assert_eq!(
            *account,
            ClientAccount {
                client: 1,
                available: 0.0,
                held: 0.0,
                total: 0.0,
                locked: true,
            }
        );
        Ok(())
    }

    #[test]
    fn output_csv_to_stdout() -> Result<(), Error> {
        // Testing stdout idea from https://jeffkreeftmeijer.com/rust-stdin-stdout-testing/
        let mut accounts: HashMap<u16, ClientAccount> = HashMap::new();
        accounts.insert(
            1,
            ClientAccount {
                client: 1,
                available: 10.0,
                held: 20.0,
                total: 30.0,
                locked: false,
            },
        );
        // check with only one account, due to arbitrary ordering of HashMap::into_values()
        let mut output: Vec<u8> = Vec::new();
        output_to_stdout(accounts, &mut output)?;
        assert_eq!(
            &output,
            b"client,available,held,total,locked\n1,10.0,20.0,30.0,false\n"
        );
        Ok(())
    }
}
