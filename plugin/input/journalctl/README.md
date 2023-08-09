# Journal.d plugin
Reads `journalctl` output.

### Config params
**`offsets_file`** *`string`* *`required`* 

The filename to store offsets of processed messages.

<br>

**`journal_args`** *`[]string`* *`default=-f -a`* 

Additional args for `journalctl`.
Plugin forces "-o json" and "-c *cursor*" or "-n all", otherwise
you can use any additional args.
> Have a look at https://man7.org/linux/man-pages/man1/journalctl.1.html

<br>

**`persistence_mode`** *`string`* *`default=async`* *`options=async|sync`* 

It defines how to save the offsets file:
*  `async` – it periodically saves the offsets using `async_interval`. The saving operation is skipped if offsets haven't been changed. Suitable, in most cases, it guarantees at least one delivery and makes almost no overhead.
*  `sync` – saves offsets as part of event commitment. It's very slow but excludes the possibility of event duplication in extreme situations like power loss.

Save operation takes three steps:
*  Write the temporary file with all offsets;
*  Call `fsync()` on it;
*  Rename the temporary file to the original one.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*