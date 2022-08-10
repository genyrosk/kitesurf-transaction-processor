use std::collections::HashMap;
use std::fs;
use std::io::prelude::*;
use std::io::BufReader;

use crate::{ClientAccount, Error, Tx};

pub fn open_file(path: &str) -> Result<BufReader<fs::File>, Error> {
    let file = fs::File::open(path).expect(&format!("Unable to open file: {}", path));
    let buf_reader = BufReader::new(file);
    Ok(buf_reader)
}

pub fn read_csv<R: std::io::Read>(buf: R) -> Result<Vec<Tx>, Error> {
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .trim(csv::Trim::All)
        .from_reader(buf);

    let mut data: Vec<Tx> = vec![];
    for result in csv_reader.deserialize() {
        let tx: Tx = result?;
        data.push(tx);
    }

    Ok(data)
}

pub fn output_to_stdout(
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::TxType;

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
