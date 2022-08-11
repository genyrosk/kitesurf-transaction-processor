Commands:

```
$ cargo test
$ cargo run -- data/input.csv > data/output.txt
```

Rust version used: 1.59.0

### Correctness

Only deposits can be reversed via a Dispute, followed by a Chargeback. Withdrawals can't be disputed.

A quick google search with the terms "cancel withdrawal" yielded

> Once a withdrawal has been sent and marked as "Success" in your account, it is impossible to cancel or reverse the transaction.

Unit tests have been written to check that the program behaves as expected.

The program will throw an error if a Deposit or Withdrawal transaction doesn't contain an amount. This behaviour is also captured in unit tests. The `main` program will ignore such errors and attempt to continue processing the rest of the transactions.

### Safety and Robustness

No unsafe Rust used in code and no unwrapping of Options or Results outside of tests.

### Efficiency

Csv read is buffered into the program

> It can be excessively inefficient to work directly with a Read instance. For example, every call to read on TcpStream results in a system call. A BufReader<R> performs large, infrequent reads on the underlying Read and maintains an in-memory buffer of the results.
