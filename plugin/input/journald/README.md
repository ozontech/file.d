# Journal.d plugin
Reads `journald` output.

### Config params
**`offsets_file`** *`string`* *`required`* 

The filename to store offsets of processed messages.

<br>

**`journal_args`** *`[]string`* *`default=-f -a`* 

Additional args for `journald`.
Plugin forces "-o json" and "-c *cursor*" or "-n all", otherwise
you can use any additional args.
> Have a look at https://man7.org/linux/man-pages/man1/journald.1.html

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*