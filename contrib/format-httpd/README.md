# Web Server Log Format Plugin

## Configuration
There are three fields which you will need to configure in order for Drill to read web server logs which are:
* **`logFormat`**:
* **`timestampFormat`**: 
* **`extensions`**:  The file extension of your web server logs.
* **`maxErrors`**:  Sets the plugin error tolerence. When set to any value less than `0`, Drill will ignore all errors. 

### Implicit Columns
Data queried by this plugin will return two implicit columns:

* **`_raw`**: This returns the raw, unparsed log line
* **`_matched`**:  Returns `true` or `false` depending on whether the line matched the config string.

Thus, if you wanted to see which lines in your log file were not matching the config, you could use the following query:

```sql
SELECT _raw
FROM <data>
WHERE _matched = false
```
