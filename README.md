# stdout-rotator

`stdout-rotator` is a command line utility which allows to direct its standard input to both standard output and a file as well applying automatic log rotation on the output file. It has been designed to be deployed in containerised applications which log to standard output to also log on a file to be consumed by log forwarders like [fluent-bit](https://fluentbit.io/).

## Build

Use standard `cargo build` for a debug build and `cargo build --release` for a release build.
