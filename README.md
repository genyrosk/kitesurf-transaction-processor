# Basics

Rust version used: 1.59.0

Commands:

```
$ cargo test
```

To run the tests.

```
$ cargo build
```

To build the binary.

```
$ cargo run -- data/input.csv
$ cargo run -- data/input.csv > data/output.txt
```

To run the program you need to pass it an input filepath. The program outputs to stdout, which you can pipe into another file.


### Correctness

Only deposits can be reversed via a Dispute, followed by a Chargeback. Withdrawals can't be disputed.

A quick google search with the terms "cancel withdrawal" yielded:

> Once a withdrawal has been sent [...], it is impossible to cancel or reverse the transaction.

Unit tests have been written to check that the program behaves as expected.

An error is raised if a Deposit or Withdrawal transaction doesn't contain an amount. This behaviour is also captured in unit tests. The `main` program will however ignore such errors and attempt to continue processing the rest of the transactions.

### Safety and Robustness

No unsafe Rust code is used and no unwrapping of Options or Results outside of tests. All possible errors raised by libraries are converted into a custom `Error` struct.

### Efficiency

The CSV read is buffered into the program using `std::io::BufReader`. From the official Rust documentation:

> It can be excessively inefficient to work directly with a Read instance. For example, every call to read on TcpStream results in a system call. A BufReader<R> performs large, infrequent reads on the underlying Read and maintains an in-memory buffer of the results.

This way we avoid loading the entire file in one go and can later extend the program to read from a TCP stream.

A further improvement could be to process each transaction as it is being read from the buffer, instead of loading all transactions into memory and then processing them. This would make the program even more memory efficient.
