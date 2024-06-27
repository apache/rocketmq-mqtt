## Setup
Download emqtt-bench with QUIC feature
```shell
wget https://github.com/emqx/emqtt-bench/releases/download/0.4.18/emqtt-bench-0.4.18-macos13-amd64-quic.zip
```

## Test Connect
```shell
./bin/emqtt_bench conn -u user0 -P secret0 -V 4 --quic true -h localhost -p 14567 -c 10
```

### Pub

```shell
./bin/emqtt_bench pub -u user0 -P secret0 -V 4 --quic true -h localhost -p 14567 -c 1 -t T_Event/test
```
