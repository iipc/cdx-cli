# cdx-cli

Command line utility for working with CDX files

##Features:

* Extract CDX files in different formats from ARC/WARC files.
* Reformat CDX file into another CDX format.

##Usage:

The utility is in its early stages and there is no distribution yet.
So to test it you need to clone and compile it yourself.

```bash
# git clone https://github.com/iipc/cdx-cli.git
# mvn clean install
```

Then you can run the utility directly from the build's target directory:

```bash
# target/appasembler/bin/cdxcli
```

Or use the packaged file:

```bash
# tar zxvf target/cdxcli-<version>-dist.tar.gz
# cdxcli-<version>-SNAPSHOT/bin/cdxcli
```

When running it with no parameters, you will get usage information.
