# Ramper

A library for fast web crawling in Clojure.

### Show me the code

Starting a crawl with which crawls at most 10000 pages.

```clj
(require '[clojure.java.io :as io]
         '[ramper.instance :as instance])

(instance/start (io/file "seed.txt") (io/file "store-dir") {:max-urls 10000})
```

### Testing your setup

Running the crawler against a local graph server we can use [BUbiNG](https://github.com/LAW-Unimi/BUbiNG). The following will start a server on port 8080 with a 100 Million sites,
average page degree 50, average depth 3 and 0.01% of sites being broken.
```bash
java -cp bubing-0.9.15.jar:bubing-0.9.15-deps/* -Xmx4G -server it.unimi.di.law.bubing.test.NamedGraphServerHttpProxy -s 100000000 -d 50 -m 3 -t 1000 -D .0001 -A1000 -
```
The precompiled jars can be found at [http://law.di.unimi.it/software/index.php?path=download/](http://law.di.unimi.it/software/index.php?path=download/).

There also is a [babashka](https://github.com/babashka/babashka) script that downloads all of these dependencies for you and launches the proxy server with the above configuration for you.

```bash
./download_bubing
```

### Customize your crawl

Ramper comes with a couple of options to customize your crawl. These are

**fetch-filter**

A filter that is applied to every url before it goes through the sieve. Let's say you would
want to only fetch urls that contain `clojure` in their name and use the https scheme.
```clj
(require '[ramper.customization :as custom])

(defn clojure-url? [url]
  (clojure.string/index-of url "clojure"))

(instance/start seed-file store-dir {:fetch-filter (every-pred custom/https? clojure-url?)}

```

**schedule-filter**

A filter that is applied to every url before the resource gets fetched (just after the sieve).
For example let's you want to only fetch a limited number of urls per domain.
```clj
(defn max-per-domain-filter [max-per-domain]
  (let [domain-to-count (atom {})]
    (fn [url]
      (let [base (url/base url)]
        (when (< (get @domain-to-count base 0) max-per-domain)
          (swap! domain-to-count update base (fnil inc 0))
          true)))))

(instance/start seed-file store-dir {:schedule-filter (max-per-domain 100)})
```
The `max-per-domain-filter` is also provided by the [customization](src/clj/ramper/customization.clj) ns.

**store-filter**

A filter that is applied before a response is stored. Suppose you want to only store sites that contain
the word "clojure".
```clj
(require '[clojure.string :as str]
         '[ramper.html-parser :as html])

(defn contains-clojure? [resp]
  (some-> resp :body html/html->text str/lower-case (clojure.string/index-of "clojure")))

(instance/start seed-file store-dir {:store-filter contains-clojure? })
```

**follow-filter**

In the same vein as above, suppose you only want to continue following the links of a page when
the it contains the word "clojure".
```clj
(instance/start seed-file store-dir {:follow-filter contains-clojure?})
```

### Compiling

When developing you need to build the java files once before jacking in.

```bash
clojure -T:build java
```

### Tests

The tests can be run with
```bash
clojure -X:test
```

If one wants to run a specific test, use the `-X` option. See also [cognitect.test-runner](https://github.com/cognitect-labs/test-runner) for options which tests to invoke.
```bash
clojure -X:test :nses [ramper.workers.parsing-thread-test]
```

### Profiling

For Java mission control to work correctly you need to set

```bash
echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid
```
## License
