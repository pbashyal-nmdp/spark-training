
```python
lines = sc.textFile("impudigree-web.log.gz")

# Figure out all the 404s (not found)
errors = lines.filter(lambda l: "404" in l)
errors.cache()

errors.count()
errors.take(10)

# Count the individual ip addresses
ipaddresses = errors.map(lambda e: e.split(" ")).map(lambda r: r[0])
ipaddresses.take(10)
ipaddresses.countByValue()

# PHP errors
phpErrors = errors.filter(lambda l: "php" in l).collect()
for e in phpErrors:
  print(e)


```
