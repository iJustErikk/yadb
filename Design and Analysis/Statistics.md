# Statistics

Users will need to measure statistics. They want accurate or great approximates in real-time. They will probably poll on an interval. They should receive this format in a prometheus-compatible format so they can plug it straight in.

## Motivation

I wanted to debug why bloom filters did not improve a (relatively) read heavy workload. I want to see if I can rule out a high false positive rate. I could do this with logging, but I think this will be simple to implement in a less hacky and more production-friendly fashion.

## Logging, Tracing

Logging and log streaming are too expensive. Logging a constant number of times per request will gravely affect performance.

## (near) Zero-cost Abstraction

If end users do not want to pay for statistics, they should not get statistics. This should be conditionally compiled given a feature flag. Any statistics they do not want to collect should be measured (we check a settings boolean for each statistic). 

## End Design

I'll introduce another statistics module. This will have a class that is instantiated by the init entrypoint. It will be a struct of Atomic Integers/Floats. On an update event, I'll check the settings to see if I should change the atomic variables and increment them if I should. On poll, I'll read the user specified statistics and compute any derived statistics (for example, filter FP rate is total missed / total). These derived statistics are not atomically measured among dependent variables. However, with moderate load and over time, they will become very accurate. The separate atomic variables allow for greater concurrency and minimize contention time (they are lock-free).

## Future Flexibility

For now, this design should work. Perhaps in the future, some statistics will need better accuracy guarantees. Maybe derived stats will need the most recent version of either dependent variables. Maybe with different storage engines, I could have B-tree or fractal-tree or LSM statistics.