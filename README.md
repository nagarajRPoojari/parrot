
# parrot


> [!NOTE]  
> parrot is an LSM based storage engine inspired by pebble, roseDB, levelsDB and many more


## Initial Setup

This section is intended to help developers and contributors get a working copy of
`parrot` on their end

<details>
<summary>
    1. Clone this repository
</summary><br>

```sh
git clone https://github.com/nagarajRPoojari/parrot
cd parrot
```
</details>

<details>
<summary>
    2. Install `golangci-lint`
</summary><br>

Install `golangci-lint` from the [official website][golangci-install] for your OS
</details>
<br>


## Local Development

This section will guide you to setup a fully-functional local copy of `parrot`
on your end and teach you how to use it! Make sure you have installed
[golangci-lint][golangci-install] before following this section!

**Note:** This section relies on the usage of [Makefile][makefile-official]. If you
can't (or don't) use Makefile, you can follow along by running the internal commands
from [`parrot's` Makefile][makefile-file] (most of which are
OS-independent)!

### Installing dependencies

To install all dependencies associated with `parrot`, run the
command

```sh
make install
```


### Using Code Formatters

Code formatters format your code to match pre-decided conventions. To run automated code
formatters, use the Makefile command

```sh
make codestyle
```

### Using Code Linters

Linters are tools that analyze source code for possible errors. This includes typos,
code formatting, syntax errors, calls to deprecated functions, potential security
vulnerabilities, and more!

To run pre-configured linters, use the command

```sh
make lint
```

### Running Tests

Tests in `parrot` can be run using following command
```sh
make test
```

<br>


## Additional Resources

### Makefile help

<details>
<summary>
    Tap for a list of Makefile commands
</summary><br>

| Target                             | Description                                      |
|------------------------------------|--------------------------------------------------|
| `make build`                       | Build the LSM binary                             |
| `make run`                         | Build and run the binary                         |
| `make test`                        | Run tests with coverage                          |
| `make coverage`                    | Generate HTML test coverage report               |
| `make race_test`                   | Run tests with Go's race detector                |
| `make benchmark read`              | Run read benchmark                               |
| `make benchmark write`             | Run both WAL and non-WAL write benchmarks        |
| `make benchmark write WAL=on`      | Run only WAL-enabled write benchmark             |
| `make benchmark write WAL=off`     | Run only non-WAL write benchmark                 |
| `make prof cpu`                    | Analyze CPU profile                              |
| `make prof mem`                    | Analyze memory profile                           |
| `make prof goroutines`             | Analyze goroutine profile                        |
| `make clean`                       | Clean build artifacts and profiling files        |

<br>
</details>

Optionally, to see a list of all Makefile commands, and a short description of what they
do, you can simply run

```sh
make help
```

Which is equivalent to;

```sh
make help
```

Both of which will list out all Makefile commands available, and a short description
of what they do!

### Running `parrot`

To run parrot, use the command

```sh
make run
```

Additionally, you can pass any additional command-line arguments (if needed) as the
argument "`q`". For example;

```sh
make run q="--help"

OR

make run q="--version"
```
<br>


<br>
