# Dogtail
Tail logs in Datadog to your local machine.

Dogtail is designed to let you use the powerful command line tools you already know and love for exploring the logs of your projects, because no matter how good Datadog is, there's no place like `$HOME`.

## Usage

Running pods, an want to be able to read logs per pod? (note the use of `RUST_LOG` causes dogtail logs to be printed to stderr)
```bash
> RUST_LOG=dogtail=info dogtail logs "service:my-service env:production" -k attributes.tags.pod_name
2023-09-28 23:37:44.521434917  INFO dogtail::tailer Returned 81 events
Waiting 5s
2023-09-28 23:37:44.52156469  INFO dogtail Started writing to file: my-service-deployment-75df6dff9-dfw2x
2023-09-28 23:37:54.714708319  INFO dogtail::tailer Returned 1 events
Waiting 5s
```

Got noisy logs? View only the unique messages
```bash
? dogtail logs "env:production service:my-service" -m stdout -s | jq .attributes.message | huniq
```

Refactoring, and want to see what logs are more noise than they're worth? This example uses nushell, and writes to file as an intermediate step because tools like `uniq` can't emit counts until the input stream ends.
```bash
> dogtail logs "env: production service:my-service" -s
# Run for a while, then kill with ctrl-c
> cat output.log | jq .attributes.message | lines | uniq -c | sort-by count
```

## Installation
```
cargo install dogtail
```

## Configuration
Dogtail needs access to a [Datadog API key and an APP key](https://docs.datadoghq.com/account_management/api-app-keys/) to query logs. These are pulled from the environment variables `DD_API_KEY` and `DD_APP_KEY` respectively.

## Usage detail:
```
> dogtail --help
Tail datadog logs to files, or stdout

Usage: dogtail [OPTIONS] <COMMAND>

Commands:
  logs
  help  Print this message or the help of the given subcommand(s)

Options:
  -d, --domain <DOMAIN>  The domain to use for the API [default: api.datadoghq.eu]
  -h, --help             Print help
  -V, --version          Print version
```

```
> dogtail logs --help
Usage: dogtail logs [OPTIONS] <QUERY_STRING>

Arguments:
  <QUERY_STRING>  A query string, the same as you would use in the UI, e.g. "service:my-service"

Options:
  -d, --domain <DOMAIN>
          The domain to use for the API [default: api.datadoghq.eu]
  -o, --output-mode <OUTPUT_MODE>
          Mode - If file, log events will be partitioned by split_key and written to files, if stdout, logs will be written to stdout [default: file] [possible values: file, stdout]
  -k, --split-key <SPLIT_KEY>
          If mode is file, this is the event attribute lookup key to use for partitioning logs. Uses json-pointer syntax, e.g. "attributes.tags.pod_name". Note that event tags are unpacked into a map, so you can use tags "attributes.tags.pod_name" for this purpose. If an event doesn't have the split key, it is written to the default file
  -f, --default-output <DEFAULT_OUTPUT>
          The place logs that can't be split by split-key will be written to. If mode is stdout, this is ignored [default: output.log]
      --format-file <FORMAT_FILE>
          A file to load a formatting config from. The formatting config is a newline separated list of json-pointer keys - each output line will be the found value of each of those keys, joined by a space. If none is provided, a default logging format of "timestamp status message" will be used
  -s, --structured
          If true, structured json will be written to the output instead of formatted logs, with one event written per line
  -h, --history <HISTORY>
          Provide a number of seconds in the past to start tailing from [default: 60]
  -t, --from <FROM>
          Run the search once, rather than tailing the logs. If this is set, `history` becomes the number of seconds after this instant to get logs from. Accepts rfc3339 timestamps, e.g. "2021-01-01T00:00:00Z"
  -h, --help
          Print help
```