zeppelin-viyadb
================

This project contains Zeppelin interpreter for querying ViyaDB instance.

### Installation

First, compile the project, and install it to your local Maven repository:

```bash
mvn clean install
```

Then, go to your Zeppelin package directory, and run:

```bash
./bin/install-interpreter.sh --name viyadb --artifact com.github.viyadb:zeppelin-viyadb:0.7.2
```

