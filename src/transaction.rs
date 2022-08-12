use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::Error;

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct Tx {
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
pub enum TxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, PartialEq)]
pub struct TxState {
    pub amount: f32,
    pub type_: TxStateType,
    pub client_id: u16,
    pub disputed: bool,
    pub charged_back: bool,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum TxStateType {
    Deposit,
    Withdrawal,
}

impl TxState {
    fn new(amount: f32, type_: TxStateType, client_id: u16) -> Self {
        Self {
            amount,
            type_,
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
pub struct ClientAccount {
    pub client: u16,
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

pub fn process_tx(
    tx: Tx,
    accounts: &mut HashMap<u16, ClientAccount>,
    tx_states: &mut HashMap<u32, TxState>,
) -> Result<(), Error> {
    let client_id = tx.client_id;
    let tx_id = tx.tx_id;
    let mut account = accounts
        .entry(client_id)
        .or_insert(ClientAccount::new(client_id));

    if account.locked == true {
        return Ok(());
    }

    match tx_states.get_mut(&tx_id) {
        Some(tx_state) => match tx.type_ {
            TxType::Deposit => {}
            TxType::Withdrawal => {}
            TxType::Dispute => {
                if tx_state.disputed == false && tx_state.type_ == TxStateType::Deposit {
                    tx_state.disputed = true;
                    tx_state.charged_back = false;
                    let amount = tx_state.amount;
                    account.available -= amount;
                    account.held += amount;
                }
            }
            TxType::Resolve => {
                if tx_state.disputed == true && tx_state.type_ == TxStateType::Deposit {
                    tx_state.disputed = false;
                    tx_state.charged_back = false;
                    let amount = tx_state.amount;
                    account.available += amount;
                    account.held -= amount;
                };
            }
            TxType::Chargeback => {
                if tx_state.disputed == true && tx_state.type_ == TxStateType::Deposit {
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
                tx_states.insert(
                    tx_id,
                    TxState::new(amount, TxStateType::Deposit, tx.client_id),
                );
                account.total += amount.abs();
                account.available += amount.abs();
            }
            TxType::Withdrawal => {
                let amount = tx.amount.ok_or(Error::new(
                    "Withdrawal transaction expected to have an amount",
                ))?;
                if amount <= account.available {
                    tx_states.insert(
                        tx_id,
                        TxState::new(-amount, TxStateType::Withdrawal, tx.client_id),
                    );
                    account.total -= amount;
                    account.available -= amount;
                }
            }
            TxType::Dispute => {}
            TxType::Resolve => {}
            TxType::Chargeback => {}
        },
    };
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

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
            Tx {
                type_: TxType::Withdrawal,
                client_id: 1,
                tx_id: 3,
                amount: Some(3.0),
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
                amount: Some(10.0),
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
    fn dispute_withdrawal_is_ignored() -> Result<(), Error> {
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
    fn deposit_without_amount_throws_error() -> Result<(), Error> {
        let mut accounts: HashMap<u16, ClientAccount> = HashMap::new();
        let mut tx_states: HashMap<u32, TxState> = HashMap::new();
        let tx = Tx {
            type_: TxType::Deposit,
            client_id: 1,
            tx_id: 1,
            amount: None,
        };
        let result = process_tx(tx, &mut accounts, &mut tx_states);

        assert_eq!(result.is_err(), true);
        Ok(())
    }

    #[test]
    fn withdrawal_without_amount_throws_error() -> Result<(), Error> {
        let mut accounts: HashMap<u16, ClientAccount> = HashMap::new();
        let mut tx_states: HashMap<u32, TxState> = HashMap::new();
        let tx = Tx {
            type_: TxType::Deposit,
            client_id: 1,
            tx_id: 1,
            amount: Some(10.0),
        };
        process_tx(tx, &mut accounts, &mut tx_states)?;
        let tx = Tx {
            type_: TxType::Withdrawal,
            client_id: 1,
            tx_id: 2,
            amount: None,
        };
        let result = process_tx(tx, &mut accounts, &mut tx_states);

        assert_eq!(result.is_err(), true);
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
}
